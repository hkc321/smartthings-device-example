cmake_minimum_required(VERSION 3.5)
project(boiler_thermostat1)
set(SDK_PATH $ENV{HOME}/st-device-sdk-c)

add_executable(boiler_thermostat1 main.c)

# Paho MQTT 라이브러리 찾기
find_library(PAHO_MQTT_C paho-mqtt3c)

set(stdk_config ${CMAKE_CURRENT_LIST_DIR}/stdkconfig.cmake)
add_subdirectory(${SDK_PATH} iotcore)
target_link_libraries(boiler_thermostat1 PUBLIC iotcore  ${PAHO_MQTT_C})
