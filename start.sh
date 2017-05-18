#!/usr/bin/env bash

web_home="/data/wherehows/web"
backend_home="/data/wherehows/backend"

cd $web_home
if [ -f "RUNNING_PID" ]; then
    rm -rf ./RUNNING_PID
fi
if [[ ! -d ./log ]]; then
    mkdir ./log
fi
nohup ./bin/wherehows -Dhttp.port=19005 > ./log/web.log 2>&1 &

cd $backend_home
if [ -f "RUNNING_PID" ]; then
    rm -rf ./RUNNING_PID
fi
if [[ ! -d ./log ]]; then
    mkdir ./log
fi
nohup ./bin/backend-service -Dhttp.port=9005 > ./log/backend.log 2>&1 &
