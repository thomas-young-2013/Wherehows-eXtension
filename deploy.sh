#!/usr/bin/env bash
app_home=`pwd`
target_dir="/data/wherehows"

# clean
cd ${target_dir}
rm -rf ./web/
rm -rf ./backend/
mkdir web
mkdir backend

# zip the files
backend="./backend-service/target/universal/stage/"
web="./web/target/universal/stage/"

cd ${app_home}
cd ${backend}
cp -rf * ${target_dir}/backend;

cd ${app_home}
cd ${web}
cp -rf * ${target_dir}/web;
