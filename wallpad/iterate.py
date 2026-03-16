import socket
import time
import paho.mqtt.client as mqtt
from queue import Queue
import signal
import sys


# MQTT 브로커 설정
BROKER_HOST = "your-host"
BROKER_PORT = 1234 # your port
USERNAME = "your-username"
PASSWORD = "your-password"
OPERATION_TOPIC = "your-operation-topic"
STATUS_TOPIC = "your-status-topic"

# TCP 서버 설정(ew11)
HOST = "your ew11 ip"
PORT = 1234 # your ew11 port

client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
send_queue = Queue() # MQTT → TCP 전송용 Queue

sock = None

def connect_tcp():
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)

            s.settimeout(2.0)
            s.connect((HOST, PORT))
            print("TCP 서버 연결 성공")
            return s
        except Exception as e:
            print(f"TCP 연결 실패: {e}")
            print("3초 후 재시도...")
            time.sleep(3)

def on_connect(client, userdata, flags, reason_code, properties):
    """연결 시 호출되는 콜백 함수"""
    if reason_code == 0:
        print(f"MQTT 브로커에 연결되었습니다: {BROKER_HOST}")
        # 토픽 구독 (필요시)
        client.subscribe(OPERATION_TOPIC, qos=1)
        print(f"토픽 '{OPERATION_TOPIC}' 구독을 시작합니다")
    else:
        print(f"MQTT 연결 실패. 오류 코드: {reason_code}")

def on_message(client, userdata, msg):
    """메시지 수신 시 호출되는 콜백 함수"""
    if (msg.topic == OPERATION_TOPIC):
        send_queue.put(msg.payload)


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    """연결 해제 시 호출되는 콜백 함수"""
    print("MQTT 브로커와 연결이 해제되었습니다")

def cleanup(signum=None, frame=None):
    print("\n종료 중...")

    try:
        client.loop_stop()
    except:
        pass

    try:
        client.disconnect()
    except:
        pass

    try:
        if sock:
            sock.close()
    except:
        pass

    print("프로그램 종료")
    sys.exit(0)

def parse_multiple_packets(raw_data: bytes):
    """
    패킷이 동시에 여러개 오는 경우 헤더(0xf7)를 기준으로 분리
    """
    packets = raw_data.split(b'\xf7')
    results = []

    for p in packets:
        if not p:  # 빈 바이트열 건너뛰기
            continue
        packet = b'\xf7' + p
        results.append(packet)

    return results

def is_all_controller_status_response(packet: bytes):
    """
    전체 보일러 컨트롤러 상태 응답인지 확인
    """
    return packet.startswith(b'\xf7\x36\x0f\x81')

def reconnect_socket():
    global sock
    print("재연결 수행 중...")
    try:
        sock.close()
    except:
        pass
    sock = connect_tcp()

signal.signal(signal.SIGINT, cleanup)
signal.signal(signal.SIGTERM, cleanup)
signal.signal(signal.SIGHUP, cleanup)

def main():
    # --- MQTT ---
    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    print("MQTT 브로커 연결 중...")
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()

    # --- TCP 초기 연결 ---
    global sock
    sock = connect_tcp()

    timeout_count = 0   # ★ timeout 누적 카운터

    packet = bytes([
                0xF7,           # 시작 바이트
                0x36,           # 명령어
                0x0F,           # 카운터 (1바이트)
                0x01,           # 데이터 타입
                0x00,           # 예약
                0xCF,           # 체크섬
                0x0C            # 종료 바이트
            ])

    try:
        while True:


            # 패킷 전송
            try:
                sock.sendall(packet)
            except Exception as e:
                print(f"상태 패킷 요청 전송 오류: {e}")
                reconnect_socket()
                continue

            # --------------------
            # 1) MQTT → TCP (Queue)
            # --------------------
            if not send_queue.empty():
                data = send_queue.get()
                try:
                    sock.sendall(data)
                    print(f"TCP 전송 ({len(data)} bytes)")
                except Exception as e:
                    print(f"TCP 송신 오류: {e}")
                    reconnect_socket()
                    continue

            # --------------------
            # 2) TCP → MQTT
            # --------------------
            try:
                response = sock.recv(1024)
                if response:
                    timeout_count = 0  # 정상 응답 → 타임아웃 카운터 리셋

                    packets = parse_multiple_packets(response)
                    for p in packets:
                        if is_all_controller_status_response(p):
                            result = client.publish(STATUS_TOPIC, p, qos=1)
                            result.wait_for_publish()

                # 서버가 정상 종료한 경우
                if response == b"":
                    print("TCP 서버가 연결을 종료했습니다.")
                    reconnect_socket()
                    timeout_count = 0
                    continue

            except socket.timeout:
                timeout_count += 1
                print(f"수신 타임아웃 ({timeout_count}회)")

                # timeout 여러 번 → EW11 세션이 죽었다고 판단하고 reconnect
                if timeout_count >= 10:  # 10번 연속 timeout이면 재연결
                    print("연속 타임아웃 → EW11 세션 끊김으로 판단. 재연결")
                    reconnect_socket()
                    timeout_count = 0
                    continue
            except Exception as e:
                print(f"TCP 수신 오류: {e}")
                reconnect_socket()
                timeout_count = 0
                continue

            time.sleep(1)

    except KeyboardInterrupt:
        print("\n종료 요청")

    finally:
        print("프로그램 종료")


if __name__ == "__main__":
    print("1초마다 TCP 바이너리 패킷 전송 중... (Ctrl+C로 중지)")
    main()
