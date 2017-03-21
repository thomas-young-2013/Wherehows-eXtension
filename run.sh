#!/usr/bin/env bash

cd ./backend-service/
chmod 777 ./runBackend
./runBackend &

cd ../web/
chmod 777 ./runWeb.sh
./runWeb.sh &

cd ..
echo "launched successfully"
