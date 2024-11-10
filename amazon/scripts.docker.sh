#!/bin/bash

# Initialize the environment
sh init.sh

# Build the Docker image
docker build --tag amazon-scraper-scripts:latest --file Dockerfile.scripts .

# Check if a working directory argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <script_category (e.g. "products")> [script_args]"
    exit 1
fi

# Set the working directory from the first argument
SCRIPT_CATEGORY="$1"
shift  # Remove the first argument so $@ contains only script arguments

# Run the Docker container
container_id=$(docker run \
    -it \
    --detach \
    --network host \
    --ipc=host \
    --env-file=./scripts/.env \
    --volume "./data:/app/data" \
    --volume "./out:/app/out" \
    --security-opt seccomp=./shared/seccomp_profile.json \
    -w "/app/scripts/$SCRIPT_CATEGORY" \
    amazon-scraper-scripts:latest \
    python3 "$@"
)
container_id=${container_id:0:12}
current_time=$(date '+%F-%H-%M-%S-%N')
logfile_name="$current_time-$container_id.log"
logfile_path="logs/$SCRIPT_CATEGORY/$logfile_name"
docker logs --follow "$container_id" > $logfile_path 2>&1

echo "Started script container of ID = $container_id"
echo "Path to log file of container = $logfile_path"
