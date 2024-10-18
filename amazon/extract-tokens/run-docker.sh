#!/bin/bash

docker stop amazon-extract-tokens
docker rm amazon-extract-tokens
docker build -t amazon-extract-tokens:latest .
docker run \
    -it \
    --name=amazon-extract-tokens \
    --ipc=host \
    -e "POSTGRESQL_CONN_STR=${POSTGRESQL_CONN_STR:-postgresql://admin:123@postgres:5432/postgres}" \
    -p 8000:8000 \
    --user pwuser \
    --security-opt seccomp=seccomp_profile.json \
    amazon-extract-tokens:latest \
    python3 main.py api
