#!/bin/bash

if [ -f run.pid ]; then
    PID=$(cat run.pid)
    kill $PID

    echo "🛑 Stopped process $PID"
    rm run.pid
else
    echo "❌ No pid file found"
fi
