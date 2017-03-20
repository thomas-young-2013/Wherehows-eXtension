#!/usr/bin/env bash

echo "clean the older one.."
rm -rf ./wherehowsX
echo "clone from the github.."
git clone https://github.com/thomas-young-2013/wherehowsX.git

echo "download completed.."

cd ./wherehowsX
./gradlew clean build dist

echo "build completed"
