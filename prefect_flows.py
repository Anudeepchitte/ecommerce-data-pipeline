#!/usr/bin/env python3
"""
Orchestration Module for E-commerce Data Pipeline using Prefect

This module implements the orchestration framework for the data pipeline using Prefect.
It defines DAGs for daily and weekly processing with dependencies and failure alerts.

Features:
- Daily processing DAG for user activity and orders
- Weekly processing DAG for inventory
- Dependency management
- Failure alerts and retry logic
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.logging import get_run_logger
from prefect.context import get_run_context
from prefect.engine.results import Result

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import modules from other components
from data_generation.user_activity_generator import generate_user_activity, save_to_json
from data_generation.orders_generator import generate_orders, save_to_csv
from data_generation.inventory_generator import generate_inventory_data, save_to_excel

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/orchestration.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("orchestration")

# Create logs directory if it doesn't exist
os.makedirs("../logs", exist_ok=True)

# Task definitions
@task(retries=3, retry_delay_seconds=60, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def generate_user_activity_data(num_users=1000, events_per_user_range=(5, 30), output_file="../data/user_activity.json"):
    """
    Generate user activity data.
    
    Args:
        num_users: Number of unique users to simulate
        events_per_user_range: Range of events per user (min, max)
        output_file: Path to output JSON file
    
    Returns:
        Path to generated file
    """
    logger = get_run_logger()
    logger.info(f"Generating user activity data with {num_users} users")
    
    # Create data directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Generate data
    start_date = datetime.now() - timedelta(days=1)  # Last day
    end_date = datetime.now()
    
    user_activities = generate_user_activity(
        num_users=num_users,
        events_per_user_range=events_per_user_range,
        start_date=start_date,
        end_date=end_date
    )
    
    # Save to file
    save_to_json(user_activities, output_file)
    logger.info(f"Generated {len(user_activities)} user activity events")
    logger.info(f"Data saved to {os.path.abspath(output_file)}")
    
    return output_file

@task(retries=3, retry_delay_seconds=60, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def generate_orders_data(num_orders=500, output_file="../data/orders.csv"):
    """
    Generate orders data.
    
    Args:
        num_orders: Number of orders to generate
        output_file: Path to output CSV file
    
    Returns:
        Path to generated file
    """
    logger = get_run_logger()
    logger.info(f"Generating orders data with {num_orders} orders")
    
    # Create data directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Generate data
    start_date = datetime.now() - timedelta(days=1)  # Last day
    end_date = datetime.now()
    
    orders = generate_orders(
        num_orders=num_orders,
        start_date=start_date,
        end_date=end_date
    )
    
    # Save to file
    save_to_csv(orders, output_file)
    logger.info(f"Generated {len(orders)} orders")
    logger.info(f"Data saved to {os.path.abspath(output_file)}")
    
    return output_file

@task(retries=3, retry_delay_seconds=60, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=7))
def generate_inventory_data(num_products=1000, output_file="../data/inventory.xlsx"):
    """
    Generate inventory data.
    
    Args:
        num_products: Number of products to generate
        output_file: Path to output XLSX file
    
    Returns:
        Path to generated file
    """
    logger = get_run_logger()
    logger.info(f"Generating inventory data with {num_products} products")
    
    # Create data directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Generate data
    as_of_date = datetime.now()
    
    inventory_data = generate_inventory_data(
        num_products=num_products,
        as_of_date=as_of_date
    )
    
    # Save to file
    save_to_excel(inventory_data, output_file)
    logger.info(f"Generated {len(inventory_data)} inventory items")
    logger.info(f"Data saved to {os.path.abspath(output_file)}")
    
    return output_file

@task(retries=3, retry_delay_seconds=60)
def ingest_user_activity_data(input_file, output_dir="../data/bronze/user_activity"):
    """
    Ingest user activity data.
    
    Args:
        input_file: Path to input JSON file
        output_dir: Path to output Parquet directory
    
    Returns:
        Path to output directory
    """
    logger = get_run_logger()
    logger.info(f"Ingesting user activity data from {input_file}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # In a real implementation, we would call the ingestion module
    # For this demo, we'll just log a message
    logger.info(f"User activity data ingested to {output_dir}")
    
    return output_dir

@task(retries=3, retry_delay_seconds=60)
def ingest_orders_data(input_file, output_dir="../data/bronze/orders"):
    """
    Ingest orders data.
    
    Args:
        input_file: Path to input CSV file
        output_dir: Path to output Parquet directory
    
    Returns:
        Path to output directory
    """
    logger = get_run_logger()
    logger.info(f"Ingesting orders data from {input_file}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # In a real implementation, we would call the ingestion module
    # For this demo, we'll just log a message
    logger.info(f"Orders data ingested to {output_dir}")
    
    return output_dir

@task(retries=3, retry_delay_seconds=60)
def ingest_inventory_data(input_file, output_dir="../data/bronze/inventory"):
    """
    Ingest inventory data.
    
    Args:
        input_file: Path to input XLSX file
        output_dir: Path to output Parquet directory
    
    Returns:
        Path to output directory
    """
    logger = get_run_logger()
    logger.info(f"Ingesting inventory data from {input_file}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # In a real implementation, we would call the ingestion module
    # For this demo, we'll just log a message
    logger.info(f"Inventory data ingested to {output_dir}")
    
    return output_dir

@task(retries=2, retry_delay_seconds=120)
def process_user_activity_data(input_dir, output_dir="../data/silver/user_activity"):
    """
    Process user activity data.
    
    Args:
        input_dir: Path to input Parquet directory
        output_dir: Path to output Parquet directory
    
    Returns:
        Path to output directory
    """
    logger = get_run_logger()
    logger.info(f"Processing user activity data from {input_dir}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # In a real implementation, we would call the processing module
    # For this demo, we'll just log a message
    logger.info(f"User activity data processed to {output_dir}")
    
    return output_dir

@task(retries=2, retry_delay_seconds=120)
def process_orders_data(input_dir, output_dir="../data/silver/orders"):
    """
    Process orders data.
    
    Args:
        input_dir: Path to input Parquet directory
        output_dir: Path to output Parquet directory
    
    Returns:
        Path to output directory
    """
    logger = get_run_logger()
    logger.info(f"Processing orders data from {input_dir}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # In a real implementation, we would call the processing module
    # For this demo, we'll just log a message
    logger.info(f"Orders data processed to {output_dir}")
    
    return output_dir

@task(retries=2, retry_delay_seconds=120)
def process_inventory_data(input_dir, output_dir="../data/silver/inventory"):
    """
    Process inventory data.
    
    Args:
        input_dir: Path to input Parquet directory
        output_dir: Path to output Parquet directory
    
    Returns:
        Path to output directory
    """
    logger = get_run_logger()
    logger.info(f"Processing inventory data from {input_dir}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # In a real implementation, we would call the processing module
    # For this demo, we'll just log a message
    logger.info(f"Inventory data processed to {output_dir}")
    
    return output_dir

@task(retries=2, retry_delay_seconds=120)
def transform_to_gold_layer(user_activity_dir, orders_dir, inventory_dir, output_dir="../data/gold"):
    """
    Transform data to gold layer.
    
    Args:
        user_activity_dir: Path to user activity silver directory
        orders_dir: Path to orders silver directory
        inventory_dir: Path to inventory silver directory
        output_dir: Path to gold layer directory
    
    Returns:
        Path to output directory
    """
    logger = get_run_logger()
    logger.info(f"Transforming data to gold layer")
    
    # Create output directories if they don't exist
    os.makedirs(f"{output_dir}/dimensions/product", exist_ok=True)
    os.makedirs(f"{output_dir}/dimensions/customer", exist_ok=True)
    os.makedirs(f"{output_dir}/dimensions/date", exist_ok=True)
    os.makedirs(f"{output_dir}/facts/sales", exist_ok=True)
    os.makedirs(f"{output_dir}/facts/user_activity", exist_ok=True)
    os.makedirs(f"{output_dir}/kpis/total_revenue", exist_ok=True)
    os.makedirs(f"{output_dir}/kpis/top_products", exist_ok=True)
    os.makedirs(f"{output_dir}/kpis/user_retention", exist_ok=True)
    
    # In a real implementation, we would call the transformation module
    # For this demo, we'll just log a message
    logger.info(f"Data transformed to gold layer at {output_dir}")
    
    return output_dir

@task(retries=1, retry_delay_seconds=60)
def optimize_storage(layer_dir):
    """
    Optimize storage for a layer.
    
    Args:
        layer_dir: Path to layer directory
    
    Returns:
        Path to layer directory
    """
    logger = get_run_logger()
    logger.info(f"Optimizing storage for {layer_dir}")
    
    # In a real implementation, we would call the storage optimization module
    # For this demo, we'll just log a message
    logger.info(f"Storage optimized for {layer_dir}")
    
    return layer_dir

@task
def send_success_notification(flow_name):
    """
    Send success notification.
    
    Args:
        flow_name: Name of the flow
    """
    logger = get_run_logger()
    logger.info(f"Flow {flow_name} completed successfully")
    
    # In a real implementation, we would send an email or Slack notification
    # For this demo, we'll just log a message
    logger.info(f"Success notification sent for {flow_name}")

@task
def send_failure_notification(flow_name, error):
    """
    Send failure notification.
    
    Args:
        flow_name: Name of the flow
        error: Error message
    """
    logger = get_run_logger()
    logger.error(f"Flow {flow_name} failed: {error}")
    
    # In a real implementation, we would send an email or Slack notification
    # For this demo, we'll just log a message
    logger.info(f"Failure notification sent for {flow_name}")

# Flow definitions
@flow(name="Daily User Activity and Orders Processing")
def daily_processing_flow():
    """Daily processing flow for user activity and orders data."""
    try:
        # Generate data
        user_activity_file = generate_user_activity_data()
        orders_file = generate_orders_data()
        
        # Ingest data
        user_activity_bronze = ingest_user_activity_data(user_activity_file)
        orders_bronze = ingest_orders_data(orders_file)
        
        # Process data
        user_activity_silver = process_user_activity_data(user_activity_bronze)
        orders_silver = process_orders_data(orders_bronze)
        
        # Transform to gold layer (requires inventory data)
        # We'll assume inventory data is already available from weekly processing
        inventory_silver = "../data/silver/inventory"
        gold_layer = transform_to_gold_layer(user_activity_silver, orders_silver, inventory_silver)
        
        # Optimize storage
        optimize_storage("../data/bronze")
        optimize_storage("../data/silver")
        optimize_storage("../data/gold")
        
        # Send success notification
        send_success_notification("daily_processing_flow")
        
        return "Success"
    
    except Exception as e:
        # Send failure notification
        send_failure_notification("daily_processing_flow", str(e))
        raise

@flow(name="Weekly Inventory Processing")
def weekly_processing_flow():
    """Weekly processing flow for inventory data."""
    try:
        # Generate data
        inventory_file = generate_inventory_data()
        
        # Ingest data
        inventory_bronze = ingest_inventory_data(inventory_file)
        
        # Process data
        inventory_silver = process_inventory_data(inventory_bronze)
        
        # Transform to gold layer (requires user activity and orders data)
        # We'll assume user activity and orders data is already available from daily processing
        user_activity_silver = "../data/silver/user_activity"
        orders_silver = "../data/silver/orders"
        gold_layer = transform_to_gold_layer(user_activity_silver, orders_silver, inventory_silver)
        
        # Optimize storage
        optimize_storage("../data/bronze")
        optimize_storage("../data/silver")
        optimize_storage("../data/gold")
        
        # Send success notification
        send_success_notification("weekly_processing_flow")
        
        return "Success"
    
    except Exception as e:
        # Send failure notification
        send_failure_notification("weekly_processing_flow", str(e))
        raise

@flow(name="E-commerce Data Pipeline")
def main_flow():
    """Main flow that orchestrates daily and weekly processing."""
    # Get current day of week
    day_of_week = datetime.now().weekday()
    
    # Run daily processing
    daily_result = daily_processing_flow()
    
    # Run weekly processing on Sundays (day_of_week = 6)
    if day_of_week == 6:
        weekly_result = weekly_processing_flow()
    
    return "Success"

if __name__ == "__main__":
    # Run the main flow
    main_flow()
