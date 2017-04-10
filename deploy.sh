#!/usr/bin/env bash
app_home=`pwd`
TARGET_SERVER="root@remote:/data/wherehows"

# zip the files
backend="./backend-service/target/universal/stage/"
web="./web/target/universal/stage/"

cd ${backend}
zip -r "backend.zip" *;
scp -r "./backend.zip" ${TARGET_SERVER}/backend/;

cd ${app_home}
cd ${web}
zip -r "web.zip" *;
scp -r "./web.zip" ${TARGET_SERVER}/web/;
