#!/bin/bash

docker stop amazon-scraping-backend
docker rm amazon-scraping-backend
docker build -t amazon-scraping-backend:latest .
docker run \
    -it \
    --name=amazon-scraping-backend \
    --ipc=host \
    --env-file=.env \
    -p 8000:8000 \
    --user pwuser \
    --security-opt seccomp=seccomp_profile.json \
    amazon-scraping-backend:latest \
    python3 main.py api
