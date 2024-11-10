#!/bin/bash

mkdir -m 777 -p data/categories
mkdir -m 777 -p data/products

mkdir -m 777 -p out/categories
mkdir -m 777 -p out/products

mkdir -m 777 -p logs/categories
mkdir -m 777 -p logs/products

docker build --tag amazon-scraper-base:latest --file Dockerfile.base .
