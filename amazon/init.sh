#!/bin/bash

mkdir -m 777 -p data
mkdir -m 777 -p out
mkdir -m 777 -p logs

docker build --tag amazon-scraper-base:latest --file Dockerfile.base .
