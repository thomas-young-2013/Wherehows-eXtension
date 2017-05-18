#!/usr/bin/env bash

lsof -i:9005 | awk '{if($1 == "java") print $2}'|uniq|xargs kill -9
lsof -i:19005 | awk '{if($1 == "java") print $2}'|uniq|xargs kill -9
