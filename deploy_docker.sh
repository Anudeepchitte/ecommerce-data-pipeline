#!/bin/bash
# Docker deployment script for E-Commerce Data Pipeline
# This script builds and deploys the pipeline using Docker containers

set -e

echo "Starting Docker deployment of E-Commerce Data Pipeline..."

# Define variables
REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || echo "/home/ubuntu/ecommerce_pipeline_repo")
DOCKER_COMPOSE_FILE="$REPO_ROOT/config/production/docker-compose.yml"
ENV_FILE="$REPO_ROOT/config/production/.env"

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "Error: Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Build Docker images
echo "Building Docker images..."
docker build -t ecommerce-pipeline:latest -f "$REPO_ROOT/config/production/Dockerfile" "$REPO_ROOT"

# Create necessary volumes
echo "Creating Docker volumes..."
docker volume create ecommerce-pipeline-data
docker volume create ecommerce-pipeline-logs

# Start services using Docker Compose
echo "Starting services with Docker Compose..."
docker-compose -f "$DOCKER_COMPOSE_FILE" --env-file "$ENV_FILE" up -d

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 10

# Run initialization if needed
echo "Running initialization..."
docker exec ecommerce-pipeline-app python -m src.data_generation.generate_data --output-dir /data/bronze --sample-size small

echo "Docker deployment completed successfully!"
echo "Access the dashboard at: http://localhost:8501"
echo "Access Airflow at: http://localhost:8080"
echo "Access Prefect at: http://localhost:4200"

echo "To stop the services: docker-compose -f $DOCKER_COMPOSE_FILE down"
