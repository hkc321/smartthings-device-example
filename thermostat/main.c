/* ***************************************************************************
 *
 * Copyright 2019 Samsung Electronics All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 ****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <string.h>
#include <time.h>
#include <sys/select.h>
#include <errno.h>
#include <math.h>
#include "MQTTClient.h"

#include <sys/prctl.h>

#include "st_dev.h"

// PROCESS_NAME
#define PROCESS_NAME "BOILER_CTRL_1"

// CONTROLLER_NUMBER
#define CONTROLLER_NUMBER 1
#define CONTROLLER_NUMBER_IN_ARRAY 0

// command packet
#define MAX_COMMAND_PACKET_SIZE 8

// ew11 packet
#define MAX_PACKETS 10
#define MAX_PACKET_SIZE 256

// mqtt
#define MQTT_CLIENT_NAME "boiler1"
#define OPERATION_TOPIC "boiler_controller_operation"
#define STATUS_TOPIC "boiler_controller"
#define MQTT_URL "mqtt:your-url"
#define MQTT_USERNAME "your-name"
#define MQTT_PASSWORD "your-password"

typedef struct {
    uint8_t data[MAX_PACKET_SIZE];
    int length;
} packet_pool_t;

typedef struct {
    packet_pool_t pool[MAX_PACKETS];
    int active_count;
} packet_manager_t;

typedef enum {
    PARSE_OK = 0,
    PARSE_TOO_SHORT = -1,
    PARSE_INVALID_HEADER = -2,
    PARSE_CHECKSUM_ERROR = -3
} parse_result_t;

typedef enum {
    POWER_ON = 1,
    POWER_OFF = 0
} boiler_controller_power_t;

typedef enum {
    MODE_HEAT,
    MODE_OUTSIDE,
    MODE_RESERVE,
    MODE_HOT_WATER,
    MODE_ERROR,
    MODE_OFF
} boiler_controller_mode_t;

typedef struct
{
    boiler_controller_mode_t mode;
    double target_temp;
    double current_temp;
    char unit;
} boiler_controller_t;

typedef struct {
    MQTTClient client;
    char broker_url[256];
    char client_id[64];
    char username[64];
    char password[64];
    int connected;
} MQTT_manager_t;

// mqtt manager
// 전역 MQTT 매니저
MQTT_manager_t g_mqtt_mgr = {0};

// smartthings sdk
IOT_CTX *ctx = NULL;
IOT_CAP_HANDLE *g_heating_setpoint_handle = NULL;
IOT_CAP_HANDLE *g_current_temperature_handle = NULL;
IOT_CAP_HANDLE *g_power_switch_handle = NULL;
IOT_CAP_HANDLE *g_health_check_handle = NULL;
static st_device_status g_device_status;

// boiler controller
boiler_controller_t* g_controllers = NULL;
int g_room_count = 0;
pthread_mutex_t g_controllers_mutex = PTHREAD_MUTEX_INITIALIZER;

// current controller_status
double g_current_temperature = 0.0;
double g_current_target_temperature = 0.0;
boiler_controller_mode_t g_current_mode = MODE_OFF;
boiler_controller_power_t g_current_on_off = POWER_OFF;

// race condition smarttings<->ew11
time_t g_last_command_time = 0;
#define COMMAND_IGNORE_DURATION 4  // 4초 동안 무시

// health check
time_t g_last_message_recieved_time = 0;

volatile sig_atomic_t is_exit = false;
volatile sig_atomic_t mqtt_reconnect_needed = 0;

int mqtt_reconnect(void);
int32_t cap_health_check_update(IOT_CAP_HANDLE *handle, const char* status);
void send_on_off_packet_to_ew11(int controller_id, boiler_controller_power_t on_off);

packet_manager_t* g_packet_manager = NULL;

/**
 * 30초마다 연결 확인
 */
void signal_handler(int sig_num)
{
    if (sig_num == SIGINT || sig_num == SIGTERM) {
        is_exit = true;
    } else if (sig_num == SIGALRM) {
        time_t now = time(NULL);
        static bool prev_device_online = true;  // 이전 상태 기억
        bool device_online = false;
        
        // 1. 실제 디바이스 연결 상태 체크
        if (now - g_last_message_recieved_time < 180) {
            device_online = true;
        }
        
        // 2. 상태 변화가 있을 때만 SmartThings에 알림
        if (device_online != prev_device_online) {
            if (device_online) {
                printf("Real device back online\n");
                // 온라인 상태로 복구
                cap_health_check_update(g_health_check_handle, "online");
            } else {
                printf("Real device went offline\n");
                // 오프라인 상태로 변경
                cap_health_check_update(g_health_check_handle, "offline");
            }
            prev_device_online = device_online;
        }

        // 3. MQTT 재연결 체크
        if (!g_mqtt_mgr.connected) {
            mqtt_reconnect_needed = 1;
        }
        
        // 항상 30초마다 확인
        alarm(30);
    }
}

void event_loop()
{
    alarm(30);

    while (!is_exit) {
        pause();
        
        // MQTT 재연결 처리
        if (mqtt_reconnect_needed) {
            mqtt_reconnect_needed = 0;
            mqtt_reconnect();
        }
    }

    printf("\nExiting...\n");
    
    MQTTClient_disconnect(g_mqtt_mgr.client, 10000);
    MQTTClient_destroy(&g_mqtt_mgr.client);
    
    printf("Cleanup complete\n");
}

static void iot_status_cb(st_device_status device_status, void *usr_data)
{
    printf("Device status %d\n", device_status);
    g_device_status = device_status;
    switch (device_status) {
        case ST_DEVICE_STATUS_INIT:
            break;
        case ST_DEVICE_STATUS_ONBOARDING_READY:
            break;
        case ST_DEVICE_STATUS_ONBOARDING_START:
            break;
        case ST_DEVICE_STATUS_ONBOARDING_NEED_CONFIRM:
            break;
        case ST_DEVICE_STATUS_ONBOARDING_ONBOARDED:
            break;
        case ST_DEVICE_STATUS_CLOUD_DISCONNECTED:
            break;
        case ST_DEVICE_STATUS_CLOUD_CONNECTED:
            break;
    }
}


void cap_switch_init_cb(IOT_CAP_HANDLE *handle, void *usr_data)
{
    int32_t sequence_no = 1;

    /* Send initial switch attribute */
    ST_CAP_SEND_ATTR_STRING(handle, "switch", "off", NULL, NULL, sequence_no);

    if (sequence_no < 0)
        printf("fail to send switch value\n");
    else {
        printf("Sequence number return : %d\n", sequence_no);
        g_current_on_off = POWER_OFF;
    }
}

int32_t cap_switch_update(IOT_CAP_HANDLE *handle, boiler_controller_power_t on_off)
{
    int32_t sequence_no = 1;

    /* Send initial switch attribute */
    ST_CAP_SEND_ATTR_STRING(handle, "switch", on_off == POWER_ON ? "on" : "off", NULL, NULL, sequence_no);

    return sequence_no;
}

void cap_thermostat_heating_setpoint_init_cb(IOT_CAP_HANDLE *handle, void *usr_data)
{
    int32_t sequence_no = 1;
    int16_t init_heating_setpoint = 30;
    const char *unit = "C";
    
    ST_CAP_SEND_ATTR_NUMBER(handle, "heatingSetpoint", init_heating_setpoint, unit, NULL, sequence_no);

    if (sequence_no < 0) {
        printf("Failed to send heatingSetpoint value\n");
    } else {
        printf("Sent heatingSetpoint, seq: %d\n", sequence_no);
        g_current_target_temperature = init_heating_setpoint;
    }
}

int32_t cap_thermostat_heating_setpoint_update(IOT_CAP_HANDLE *handle, double heating_setpoint)
{
    int32_t sequence_no = 1;
    const char *unit = "C";

    ST_CAP_SEND_ATTR_NUMBER(handle, "heatingSetpoint", heating_setpoint, unit, NULL, sequence_no);

    return sequence_no;
}

void cap_temperature_measurement_temperature_init_cb(IOT_CAP_HANDLE *handle, void *usr_data)
{
    int32_t sequence_no = 1;
    int16_t init_current_temperature = 23;
    const char *unit = "C";
    
    ST_CAP_SEND_ATTR_NUMBER(handle, "temperature", init_current_temperature, unit, NULL, sequence_no);

    if (sequence_no < 0) {
        printf("Failed to send temperature\n");
    } else {
        printf("Sent temperature, seq: %d\n", sequence_no);
        g_current_temperature = init_current_temperature;
    }
}

int32_t cap_temperature_measurement_temperature_update(IOT_CAP_HANDLE *handle, double current_temp)
{
    int32_t sequence_no = 1;
    const char *unit = "C";

    ST_CAP_SEND_ATTR_NUMBER(handle, "temperature", current_temp, unit, NULL, sequence_no);

    return sequence_no;
}

void cap_health_check_init_cb(IOT_CAP_HANDLE *handle, void *usr_data)
{
    int32_t sequence_no = 1;
    
    ST_CAP_SEND_ATTR_STRING(handle, "healthStatus", "online", NULL, NULL, sequence_no);

    if (sequence_no < 0) {
        printf("Failed to send healthStatus\n");
    } else {
        printf("Sent healthStatus, seq: %d\n", sequence_no);
    }
}

int32_t cap_health_check_update(IOT_CAP_HANDLE *handle, const char* status)
{
    if (strcmp(status, "online") != 0 && strcmp(status, "offline") != 0) {
        printf("status can only be online or offline\n");
        return -1;
    }

    int32_t sequence_no = 1;
    
    ST_CAP_SEND_ATTR_STRING(handle, "healthStatus", (char *)status, NULL, NULL, sequence_no);

    return sequence_no;
}

void cap_switch_cmd_off_cb(IOT_CAP_HANDLE *handle,
                           iot_cap_cmd_data_t *cmd_data, void *usr_data)
{
    int32_t sequence_no = 1;

    printf("OFF command received");

    /* Update switch attribute */
    ST_CAP_SEND_ATTR_STRING(handle, "switch", "off", NULL, NULL, sequence_no);

    if (sequence_no < 0)
        printf("fail to send switch value\n");
    else {
        printf("Sequence number return : %d\n", sequence_no);

        send_on_off_packet_to_ew11(CONTROLLER_NUMBER, POWER_OFF);
        g_last_command_time = time(NULL);
    }
}

void cap_switch_cmd_on_cb(IOT_CAP_HANDLE *handle,
                          iot_cap_cmd_data_t *cmd_data, void *usr_data)
{
    int32_t sequence_no = 1;

    printf("ON command received");

    /* Update switch attribute */
    ST_CAP_SEND_ATTR_STRING(handle, "switch", "on", NULL, NULL, sequence_no);

    if (sequence_no < 0)
        printf("fail to send switch value\n");
    else {
        printf("Sequence number return : %d\n", sequence_no);
        send_on_off_packet_to_ew11(CONTROLLER_NUMBER, POWER_ON);
        g_last_command_time = time(NULL);
    }  
}

float decode_temp(uint8_t byte_val) {
    /*
     * 7bit: 0.5 여부 -> 1: true, 0: false
     * 6 ~ 0 bit: 온도 -> 0 ~ 127°C
     */
    return (byte_val & 0x7F) + ((byte_val >> 7) & 0x01) * 0.5f;
}

int encode_temp(double temp) {
    int intPart = (int)floor(temp);
    int isHalf = (temp - intPart == 0.5) ? 1 : 0;
    return intPart + (isHalf ? 128 : 0);
}

uint8_t xor_checksum(uint8_t *data, int length) {
    uint8_t checksum = 0;
    for (int i = 0; i < length; i++) {
        checksum ^= data[i];
    }
    return checksum;
}

// ADD 체크섬 계산 (합계의 하위 1바이트)
uint8_t add_checksum(uint8_t *data, int length) {
    uint16_t sum = 0;  // 오버플로우 방지를 위해 16비트 사용
    for (int i = 0; i < length; i++) {
        sum += data[i];
    }
    return (uint8_t)(sum % 256);  // 하위 1바이트만 반환
}

int publish_message(const char* topic, const uint8_t* message, size_t message_len) {
    if (!g_mqtt_mgr.connected) {
        printf("MQTT가 연결되지 않았습니다\n");
        return -1;
    }

    // 먼저 복사
    uint8_t buffer[256];
    memcpy(buffer, message, message_len);

    MQTTClient_message pubmsg = MQTTClient_message_initializer;
    pubmsg.payload = (void*)buffer;
    pubmsg.payloadlen = message_len;
    pubmsg.qos = 1;
    

    int rc = MQTTClient_publishMessage(g_mqtt_mgr.client, topic, &pubmsg, NULL);
    if (rc == MQTTCLIENT_SUCCESS) {
        printf("바이트 데이터 발행 성공: %zu bytes\n", message_len);
    }
    
    return rc;
}

void send_target_temp_packet_to_ew11(int controller_id, double target_temp)
{
    uint8_t packet[MAX_COMMAND_PACKET_SIZE];
    
    packet[0] = 0xf7;
    packet[1] = 0x36;
    packet[2] = controller_id;
    packet[3] = 0x44;
    packet[4] = 0x01;
    packet[5] = encode_temp(target_temp);
    packet[6] = xor_checksum(packet, 6);
    packet[7] = add_checksum(packet, 7);

    publish_message(OPERATION_TOPIC, packet, sizeof(packet));
}

void send_on_off_packet_to_ew11(int controller_id, boiler_controller_power_t on_off)
{
    uint8_t packet[MAX_COMMAND_PACKET_SIZE];
    
    packet[0] = 0xf7;
    packet[1] = 0x36;
    packet[2] = controller_id;
    packet[3] = 0x50;
    packet[4] = 0x01;
    packet[5] = on_off == POWER_ON ? 0x00 : 0x01;
    packet[6] = xor_checksum(packet, 6);
    packet[7] = add_checksum(packet, 7);

    publish_message(OPERATION_TOPIC, packet, sizeof(packet));
}

void cap_thermostat_heating_setpoint_cmd_set_heating_setpoint_cb(IOT_CAP_HANDLE *handle, iot_cap_cmd_data_t *cmd_data, void *usr_data)
{
    printf("heatingSetpoint command reveived");

    // 첫 번째 인자에서 설정 온도 값 추출
    if (cmd_data->num_args > 0) {
        double setpoint_value = cmd_data->cmd_data[0].number;

        printf("Received heatingSetpoint: %.1f\n",setpoint_value);

        // 받은 값을 그대로 서버에 업데이트
        int32_t result = cap_thermostat_heating_setpoint_update(handle, setpoint_value);
        if (result < 0) {
            printf("Failed to send heatingSetpoint value\n");
        } else {
            printf("Sent heatingSetpoint, seq: %d\n", result);
            g_current_target_temperature = setpoint_value;
            send_target_temp_packet_to_ew11(CONTROLLER_NUMBER, setpoint_value);
            g_last_command_time = time(NULL);
        }
    }
}

// 바이너리 데이터를 헥스로 출력하는 함수
void print_hex(const uint8_t* data, int len, const char* prefix) {
    printf("%s (BYTES): ", prefix);
    for (int i = 0; i < len; i++) {
        printf("\\x%02x", data[i]);
    }
    printf("\n");
    
    printf("%s (HEX): ", prefix);
    for (int i = 0; i < len; i++) {
        printf("%02x", data[i]);
        if (i < len - 1) printf(" ");
    }
    printf("\n\n");
}

bool is_current_status_packet(const uint8_t* packet)
{
    return packet[0] == 0xF7 && packet[1] == 0x36 && packet[2] == 0x0F && packet[3] == 0x81;
}

void parse_mode_from_packet(const uint8_t* state_packet, int result_size, boiler_controller_t controllers[])
{
    int error_control_byte = state_packet[0];
    int heat_control_byte = state_packet[1];
    int outside_control_byte = state_packet[2];
    int reserve_control_byte = state_packet[3];
    int hot_water_control_byte = state_packet[4];
    
    for (size_t i = 0; i < result_size; i++)
    {
        uint8_t bit = 1 << i;
        boiler_controller_mode_t mode = MODE_OFF;

        if (heat_control_byte & bit) {
            mode = MODE_HEAT;
        } else if (outside_control_byte & bit) {
            mode = MODE_OUTSIDE;
        } else if (reserve_control_byte & bit) {
            mode = MODE_RESERVE;
        } else if (hot_water_control_byte & bit) {
            mode = MODE_HOT_WATER;
        }

        if (error_control_byte != 0) {
            controllers[i].mode = MODE_ERROR;
        } else {
            controllers[i].mode = mode;
        }
    }
}

// 패킷 매니저 초기화 (한 번만)
packet_manager_t* init_packet_manager() {
    packet_manager_t* manager = malloc(sizeof(packet_manager_t));
    memset(manager, 0, sizeof(packet_manager_t));
    return manager;
}

// 패킷 매니저 리셋 (매 응답마다)
void reset_packet_manager(packet_manager_t* manager) {
    manager->active_count = 0;
}

// 패킷 추가 (메모리 할당 없음)
int add_packet_to_pool(packet_manager_t* manager, const uint8_t* data, int length) {
    if (manager->active_count >= MAX_PACKETS || length > MAX_PACKET_SIZE) {
        return -1; // 실패
    }
    
    packet_pool_t* packet = &manager->pool[manager->active_count];
    packet->length = length;
    memcpy(packet->data, data, length);
    
    return manager->active_count++;
}

// F7 패킷 분리 (메모리 할당 없음)
int split_packets(packet_manager_t* manager, const uint8_t* tcp_data, int data_len) {
    reset_packet_manager(manager);
    
    for (int i = 0; i < data_len; i++) {
        if (tcp_data[i] == 0xF7) {
            int packet_end = i + 1;
            
            // 다음 F7 찾기
            while (packet_end < data_len && tcp_data[packet_end] != 0xF7) {
                packet_end++;
            }
            
            int packet_length = packet_end - i;
            
            // 패킷 추가
            if (add_packet_to_pool(manager, &tcp_data[i], packet_length) < 0) {
                break;
            }
            
            // 다음 F7이 있으면 그 직전까지, 없으면 현재 위치에서 계속
            if (packet_end < data_len) {
                i = packet_end - 1;  // 다음 F7 직전으로 이동
            } else {
                break;  // 데이터 끝에 도달
            }
        }
    }
    
    return manager->active_count;
}

// 패킷 접근
packet_pool_t* get_packet(packet_manager_t* manager, int index) {
    if (index >= 0 && index < manager->active_count) {
        return &manager->pool[index];
    }
    return NULL;
}

parse_result_t parse_current_status_packet(const uint8_t* packet, int packet_size)
{
    if (packet_size < 10) {
        return PARSE_TOO_SHORT;
    }

    if (packet[0] != 0xF7) {
        return PARSE_INVALID_HEADER;
    }
    
    int data_packet_start_index = 5;
    int status_data_packet_end_index = 9;
    uint8_t data_length_hex = packet[4];
    int data_packet_length = data_length_hex;
    uint8_t room_data_packet[data_packet_length];
    uint8_t status_data_packet[5];
    int room_count = (data_packet_length - (status_data_packet_end_index - data_packet_start_index + 1)) / 2;

    if (g_controllers != NULL) {
      free(g_controllers);  // 기존 메모리 해제
    }

    g_controllers = (boiler_controller_t*)malloc(sizeof(boiler_controller_t) * room_count);
    g_room_count = room_count;
    
    for (int i = data_packet_start_index; i < data_packet_length + data_packet_start_index; i++) {
        if (i <= status_data_packet_end_index) {
            status_data_packet[i-data_packet_start_index] = packet[i];
        } else {
            room_data_packet[i-status_data_packet_end_index-1] = packet[i];
        }
    }

    parse_mode_from_packet(status_data_packet, g_room_count, g_controllers);

    for (size_t i = 0; i < g_room_count; i++)
    {
        g_controllers[i].target_temp = decode_temp(room_data_packet[i*2]);
        g_controllers[i].current_temp = decode_temp(room_data_packet[i*2+1]);
        g_controllers[i].unit = 'C';
    }

    return PARSE_OK;
}

void iot_noti_cb(iot_noti_data_t *noti_data, void *noti_usr_data)
{
    printf("Notification message received\n");

    if (noti_data->type == IOT_NOTI_TYPE_DEV_DELETED) {
        printf("[device deleted]\n");
    } else if (noti_data->type == IOT_NOTI_TYPE_RATE_LIMIT) {
        printf("[rate limit] Remaining time:%d, sequence number:%d\n",
               noti_data->raw.rate_limit.remainingTime, noti_data->raw.rate_limit.sequenceNumber);
    }
}

// MQTT 메시지 도착 콜백
int on_message_arrived(void *context, char *topicName, 
                       int topicLen, MQTTClient_message *message) {
    g_last_message_recieved_time = time(NULL);

    int packet_count = split_packets(g_packet_manager, (uint8_t*)message->payload, message->payloadlen);

    for (int i = 0; i < packet_count; i++) {
        packet_pool_t* packet = get_packet(g_packet_manager, i);
        print_hex(packet->data, packet->length, "받은 패킷");
        if (is_current_status_packet(packet->data) == true) {
            pthread_mutex_lock(&g_controllers_mutex);

            parse_result_t parse_result = parse_current_status_packet(packet->data, packet->length);


            if (parse_result == PARSE_OK) {
                // 명령 후 일정 시간은 EW11 응답 무시. 변경되지 않은 이전 상태값 전송 방지
                time_t now = time(NULL);
                bool ignore_ew11_update = (now - g_last_command_time) < COMMAND_IGNORE_DURATION;

                if (g_device_status == ST_DEVICE_STATUS_CLOUD_CONNECTED) {
                    if (g_controllers[CONTROLLER_NUMBER_IN_ARRAY].current_temp != g_current_temperature) {
                        int32_t result = cap_temperature_measurement_temperature_update(g_current_temperature_handle, g_controllers[CONTROLLER_NUMBER_IN_ARRAY].current_temp);
                        if (result < 0) {
                            printf("Failed to send temperature\n");
                        } else {
                            printf("Sent temperature, seq: %d\n", result);
                            g_current_temperature = g_controllers[CONTROLLER_NUMBER_IN_ARRAY].current_temp;
                        }
                    }

                    if (!ignore_ew11_update && g_controllers[CONTROLLER_NUMBER_IN_ARRAY].target_temp != g_current_target_temperature) {
                        int32_t result = cap_thermostat_heating_setpoint_update(g_heating_setpoint_handle, g_controllers[CONTROLLER_NUMBER_IN_ARRAY].target_temp);
                        if (result < 0) {
                            printf("Failed to send heatingSetpoint value\n");
                        } else {
                            printf("Sent heatingSetpoint, seq: %d\n", result);
                            g_current_target_temperature = g_controllers[CONTROLLER_NUMBER_IN_ARRAY].target_temp;
                        }
                    }

                    if (!ignore_ew11_update && g_controllers[CONTROLLER_NUMBER_IN_ARRAY].mode != g_current_mode) {
                        boiler_controller_power_t new_power = (g_controllers[CONTROLLER_NUMBER_IN_ARRAY].mode == MODE_OFF) ? POWER_OFF : POWER_ON;

                        if (new_power != g_current_on_off) {
                            int32_t result = cap_switch_update(g_power_switch_handle, new_power);
                            if (result < 0) {
                                printf("Failed to send switch value\n");
                            } else {
                                printf("Sent switch, seq: %d\n", result);
                                g_current_on_off = new_power;
                            }
                        }
                        g_current_mode = g_controllers[CONTROLLER_NUMBER_IN_ARRAY].mode;
                    }
                }
            }

            pthread_mutex_unlock(&g_controllers_mutex);
        }
    }

    MQTTClient_freeMessage(&message);
    MQTTClient_free(topicName);
    return 1;
}

// connection_lost callback
void on_connection_lost(void *context, char *cause) {
    printf("Connection lost: %s\n", cause);
    g_mqtt_mgr.connected = 0;
}

int mqtt_reconnect() {
    printf("MQTT 재연결 시도 중...\n");
    
    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
    conn_opts.keepAliveInterval = 20;
    conn_opts.cleansession = 1;
    conn_opts.username = g_mqtt_mgr.username;
    conn_opts.password = g_mqtt_mgr.password;
    
    int result = MQTTClient_connect(g_mqtt_mgr.client, &conn_opts);
    
    if (result == MQTTCLIENT_SUCCESS) {
        printf("MQTT 재연결 성공\n");
        g_mqtt_mgr.connected = 1;
        MQTTClient_subscribe(g_mqtt_mgr.client, STATUS_TOPIC, 1);
        return 0;
    } else {
        printf("MQTT 재연결 실패: %d\n", result);
        return -1;
    }
}

// mqtt init
int setup_mqtt() {
    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;

    strcpy(g_mqtt_mgr.broker_url, MQTT_URL);
    strcpy(g_mqtt_mgr.client_id, MQTT_CLIENT_NAME);
    strcpy(g_mqtt_mgr.username, MQTT_USERNAME);
    strcpy(g_mqtt_mgr.password, MQTT_PASSWORD);

    int rc = MQTTClient_create(&g_mqtt_mgr.client, g_mqtt_mgr.broker_url, 
                              g_mqtt_mgr.client_id, MQTTCLIENT_PERSISTENCE_NONE, NULL);
    
    if (rc != MQTTCLIENT_SUCCESS) {
        printf("MQTTClient_create 실패: %d\n", rc);
        return -1;
    }

    
    MQTTClient_setCallbacks(g_mqtt_mgr.client, NULL, on_connection_lost, on_message_arrived, NULL);
    
    conn_opts.keepAliveInterval = 20;
    conn_opts.cleansession = 1;
    conn_opts.username = g_mqtt_mgr.username;
    conn_opts.password = g_mqtt_mgr.password;
    
    int result = MQTTClient_connect(g_mqtt_mgr.client, &conn_opts);
    printf("연결 결과: %d\n", result);
    
    if (result == MQTTCLIENT_SUCCESS) {
        g_mqtt_mgr.connected = 1;
        MQTTClient_subscribe(g_mqtt_mgr.client, STATUS_TOPIC, 1);
        return 0;
    }

    g_mqtt_mgr.connected = 0;
    return -1;
}

void set_up_smartthings()
{
    st_device_config_t dev_config = {
        .device_id = "your-device_id",
        .server_type = SERVER_TYPE_AP_NORTH_EAST2,
        .id_method = ST_IDENTITY_METHOD_MANUAL_ED25519,
        .identity = {
                .ed25519 = {
                        .sn = "your-serial_number",
                        .pubkey = "your-public_key",
                        .prikey = "your-private_key",
                },
        },
        .mnId = "your_mnId",
    };

    IOT_CAP_HANDLE *switch_handle = NULL;
    IOT_CAP_HANDLE *heating_point_handle = NULL;
    IOT_CAP_HANDLE *current_temperature_handle = NULL;
    IOT_CAP_HANDLE *health_check_handle = NULL;
    int iot_err;

    // 1. create a iot context
    ctx = st_device_init(&dev_config);
    if (ctx != NULL) {
        iot_err = st_conn_set_noti_cb(ctx, iot_noti_cb, NULL);
        if (iot_err)
            printf("fail to set notification callback function\n");

        // 2. create a handle to process capability
        // implement init_callback function (cap_switch_init_cb)
        // We regard you have switch capability in your profile
        switch_handle = st_cap_handle_init(ctx, "main", "switch", cap_switch_init_cb, NULL);
        heating_point_handle = st_cap_handle_init(ctx, "main", "thermostatHeatingSetpoint", cap_thermostat_heating_setpoint_init_cb, NULL);
        current_temperature_handle = st_cap_handle_init(ctx, "main", "temperatureMeasurement", cap_temperature_measurement_temperature_init_cb, NULL);
        health_check_handle = st_cap_handle_init(ctx, "main", "healthCheck", cap_health_check_init_cb, NULL);

        // Store handles globally for TCP thread access
        g_power_switch_handle = switch_handle;
        g_heating_setpoint_handle = heating_point_handle;
        g_current_temperature_handle = current_temperature_handle;
        g_health_check_handle = health_check_handle;

        // 3. register a callback function to process capability command when it comes from the SmartThings Server
        iot_err = st_cap_cmd_set_cb(switch_handle, "off", cap_switch_cmd_off_cb, NULL);
        if (iot_err)
            printf("fail to set cmd_cb for off\n");

        iot_err = st_cap_cmd_set_cb(switch_handle, "on", cap_switch_cmd_on_cb, NULL);
        if (iot_err)
            printf("fail to set cmd_cb for on\n");

        iot_err = st_cap_cmd_set_cb(heating_point_handle, "setHeatingSetpoint", cap_thermostat_heating_setpoint_cmd_set_heating_setpoint_cb, NULL);
        if (iot_err) {
            printf("fail to set cmd_cb for heatingSetpoint\n");
        }
    } else {
        printf("fail to create the iot_context\n");
    }

    // 4. process on-boarding procedure. There is nothing more to do on the app side than call the API.
    st_conn_start(ctx, (st_status_cb)&iot_status_cb, NULL, NULL);
}

void main(void)
{
    prctl(PR_SET_NAME, PROCESS_NAME, 0, 0, 0);

    g_packet_manager = init_packet_manager();

    set_up_smartthings();

    // exit by using Ctrl+C
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGALRM, signal_handler);  // add timer signal

    g_last_message_recieved_time = time(NULL);
    setup_mqtt();
    event_loop();
}
