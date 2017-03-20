#!/usr/bin/env bash

cd ./backend-service/
./runBackend

cd ../web/
./runWeb.sh

cd ..

echo "launched successfully"
