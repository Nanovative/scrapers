#!/bin/bash

docker stop amazon-resource-manager
docker rm amazon-resource-manager
docker build -t amazon-resource-manager:latest .
docker run \
    -it \
    --name=amazon-resource-manager \
    --ipc=host \
    -e "POSTGRESQL_CONN_STR=${POSTGRESQL_CONN_STR:-postgresql://admin:123@postgres:5432/postgres}" \
    -p 8000:8000 \
    --user pwuser \
    --security-opt seccomp=seccomp_profile.json \
    amazon-resource-manager:latest \
    python3 main.py api
