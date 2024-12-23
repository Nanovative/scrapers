export MAX_PRODUCTS_PER_CATEGORY=20000

# export DEPTHS_TO_RUN=6,5,4
# export COMPOSE_PROJECT_NAME=scraper-6-5-4

export DEPTHS_TO_RUN=3,2,1
export COMPOSE_PROJECT_NAME=scraper-3-2-1

# export DEPTHS_TO_RUN=10,9,8,7
# export COMPOSE_PROJECT_NAME=scraper-10-9-8-7

bash scrape.compose.sh 
