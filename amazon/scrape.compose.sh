#!/bin/bash
bash init.sh

current_time=$(date '+%F-%H-%M-%S-%N')
default_compose_project_name="scraper-$current_time"

COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-$default_compose_project_name}
DEPTHS_TO_RUN=${DEPTHS_TO_RUN:--1}

echo "Starting scraper stack $COMPOSE_PROJECT_NAME: $DEPTHS_TO_RUN"

DEPTHS_TO_RUN=$DEPTHS_TO_RUN docker compose -p $COMPOSE_PROJECT_NAME up --detach --build
