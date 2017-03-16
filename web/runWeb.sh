#!/usr/bin/env bash

export _JAVA_OPTIONS="-Xms1024m -Xmx2048m -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
activator run -Dhttp.port=19005
