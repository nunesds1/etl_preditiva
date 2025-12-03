#!/usr/bin/env bash
set -e

if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

echo "🚀 Starting the full ETL stack..."
docker-compose up -d --build

echo "📦 Containers running:"
docker-compose ps
