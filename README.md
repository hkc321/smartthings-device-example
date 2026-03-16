# smartthings-device-example
블로그에서 작성한 글의 시스템 구조에서 가상 월패드와 가상 온도조절기의 예시 코드입니다.<br/><br/><br/>
<img width="784" height="442" alt="Image" src="https://github.com/user-attachments/assets/273023be-278e-4259-a820-eeca3aae6fb4" />

## 사전 준비
월패드의 경우 mqtt 라이브러리 설치가 필요합니다
```bash
virtualenv paho-mqtt
source paho-mqtt/bin/activate
pip install paho-mqtt
```

온도조절기의 경우 mqtt 라이브러리와 smartthings sdk 설치가 필요합니다.
```bash
git clone https://github.com/eclipse-paho/paho.mqtt.c.git
cd paho.mqtt.c
sudo apt install libssl-dev cmake
sudo make install


git clone https://github.com/SmartThingsCommunity/st-device-sdk-c.git
cd st-device-sdk-c/
git submodule update --init --recursive
```

## 실행방법
run.sh 파일을 이용해 실행할 수 있습니다.


