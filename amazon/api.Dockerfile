FROM mcr.microsoft.com/playwright/python:v1.47.0-noble

WORKDIR /app

COPY api/requirements.txt .
RUN pip install --no-cache-dir --requirement requirements.txt

COPY . .

WORKDIR /app/api
