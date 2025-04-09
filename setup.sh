#!/bin/bash
# Installation and setup script for E-commerce Data Pipeline Validation Extension

echo "Setting up E-commerce Data Pipeline Validation Extension..."

# Create virtual environment if it doesn't exist
if [ ! -d "../venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv ../venv
fi

# Activate virtual environment
source ../venv/bin/activate

# Install required packages
echo "Installing required packages..."
pip install great_expectations pandas numpy pyspark faker python-dateutil pytz

# Initialize Great Expectations if not already initialized
if [ ! -d "../great_expectations" ]; then
    echo "Initializing Great Expectations..."
    mkdir -p ../great_expectations
    great_expectations init
fi

# Create necessary directories
echo "Creating necessary directories..."
mkdir -p ../logs
mkdir -p ../data_validation/test_data
mkdir -p ../data_validation/test_results
mkdir -p ../data_validation/config
mkdir -p ../data_validation/cache
mkdir -p ../data_validation/alerts
mkdir -p ../data_validation/alerts/history

echo "Setup complete!"
echo "To run validation, use: python main.py validate --layer <layer> --dataset <dataset> --data-path <path>"
echo "To run optimized validation, use: python main.py optimize --layer <layer> --dataset <dataset> --data-path <path>"
echo "To run tests, use: python main.py test"
