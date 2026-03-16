#!/bin/bash
set -e

cmake -B build
cd build/
make
nohup ./boiler_thermostat1 > boiler_thermostat1.log 2>&1 &
echo $! > boiler_thermostat1.pid

echo "Example started with PID: $(cat boiler_thermostat1.pid)"
echo "Log file: $(pwd)/boiler_thermostat1.log"
echo "To view logs: tail -f $(pwd)/boiler_thermostat1.log"
echo "To stop: kill \$(cat boiler_thermostat1.pid)"
