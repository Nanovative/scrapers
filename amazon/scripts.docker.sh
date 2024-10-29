#!/bin/bash

# Initialize the environment
sh init.sh

# Build the Docker image
docker build --tag amazon-scraper-scripts:latest --file Dockerfile.scripts .

# Check if a working directory argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <working_directory> [script_args]"
    exit 1
fi

# Set the working directory from the first argument
WORKING_DIR="$1"
shift  # Remove the first argument so $@ contains only script arguments

# Run the Docker container
container_id=$(docker run \
    -it \
    --detach \
    --rm \
    --network host \
    --ipc=host \
    --env-file=./scripts/.env \
    --volume ./data:/app/data \
    --volume ./out:/app/out \
    --security-opt seccomp=./shared/seccomp_profile.json \
    -w "$WORKING_DIR" \
    amazon-scraper-scripts:latest \
    python3 main.py "$@"
)
current_time=$(date '+%F-%H-%M-%S-%N')
docker logs --follow "$container_id" > "logs/$current_time.log" 2>&1 &

echo $container_id
