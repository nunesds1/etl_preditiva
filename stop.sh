#!/usr/bin/env bash
set -e

echo "🛑 Stopping and removing containers..."
docker-compose down -v
