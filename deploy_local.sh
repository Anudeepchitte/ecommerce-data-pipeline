#!/bin/bash
# Local deployment script for E-Commerce Data Pipeline
# This script sets up the pipeline for local development and testing

set -e

echo "Starting local deployment of E-Commerce Data Pipeline..."

# Define variables
REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || echo "/home/ubuntu/ecommerce_pipeline_repo")
VENV_DIR="$REPO_ROOT/venv"
DATA_DIR="$REPO_ROOT/data"
LOG_DIR="$REPO_ROOT/logs"

# Create necessary directories
echo "Creating necessary directories..."
mkdir -p "$DATA_DIR"/{bronze,silver,gold}
mkdir -p "$LOG_DIR"
mkdir -p "$REPO_ROOT/great_expectations"

# Set up virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Setting up virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

# Activate virtual environment
source "$VENV_DIR/bin/activate"

# Install dependencies
echo "Installing dependencies..."
pip install -r "$REPO_ROOT/requirements.txt"

# Initialize data directories with sample data
echo "Initializing data directories with sample data..."
python -m src.data_generation.generate_data --output-dir "$DATA_DIR/bronze" --sample-size small

# Set up configuration
echo "Setting up configuration..."
cp "$REPO_ROOT/config/development/pipeline_config.yaml" "$REPO_ROOT/config/active_config.yaml"

# Initialize Great Expectations
echo "Initializing Great Expectations..."
cd "$REPO_ROOT" && great_expectations init

# Start services
echo "Starting services..."
python -m src.orchestration.start_services --local

echo "Local deployment completed successfully!"
echo "To run the pipeline: python -m src.orchestration.run_pipeline --config config/active_config.yaml"
echo "To view the dashboard: python -m src.visualization.streamlit_dashboard"

# Return to original directory
cd - > /dev/null
