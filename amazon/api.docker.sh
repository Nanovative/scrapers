#!/bin/bash

docker stop amazon-scraper-api
docker rm amazon-scraper-api
docker build --tag amazon-scraper-api:latest --file api.Dockerfile .
docker run \
    -it \
    --name=amazon-scraper-api \
    --ipc=host \
    --env-file=./api/.env \
    -p 8000:8000 \
    --user pwuser \
    --security-opt seccomp=./shared/seccomp_profile.json \
    amazon-scraper-api:latest \
    python3 main.py
