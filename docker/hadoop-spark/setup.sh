#!/bin/bash

if [[ "$(docker images -q cs6240/hadoop:latest 2> /dev/null)" != "" ]]; then
    docker rmi cs6240/hadoop:latest
fi

if [[ "$(docker images -q cs6240/spark:latest 2> /dev/null)" != "" ]]; then
    docker rmi cs6240/spark:latest
fi

docker build --tag cs6240/hadoop --target hadoop .
docker build --tag cs6240/spark --target spark .