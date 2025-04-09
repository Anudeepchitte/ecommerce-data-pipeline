#!/usr/bin/env python3
"""
Data Storage Module for E-commerce Data Pipeline

This module implements the lakehouse model with bronze, silver, and gold layers.
It handles data partitioning, storage optimization, and Parquet format implementation.

Features:
- Bronze layer for raw data
- Silver layer for cleaned and normalized data
- Gold layer for business-ready data
- Partitioning strategy
- Parquet format with optimization
"""

import os
import logging
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/storage.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("data_storage")

# Create logs directory if it doesn't exist
os.makedirs("../logs", exist_ok=True)

# Define storage structure
STORAGE_STRUCTURE = {
    "bronze": {
        "user_activity": {"partitions": ["event_type"]},
        "orders": {"partitions": ["order_status"]},
        "inventory": {"partitions": ["category"]}
    },
    "silver": {
        "user_activity": {"partitions": ["event_date", "event_type"]},
        "orders": {"partitions": ["order_year", "order_month", "order_status"]},
        "inventory": {"partitions": ["category", "stock_status"]}
    },
    "gold": {
        "dimensions": {
            "product": {"partitions": ["category"]},
            "customer": {"partitions": []},
            "date": {"partitions": ["year", "month"]}
        },
        "facts": {
            "sales": {"partitions": ["order_year", "order_month"]},
            "user_activity": {"partitions": ["activity_year", "activity_month", "event_type"]}
        },
        "kpis": {
            "total_revenue": {
                "by_date": {"partitions": []},
                "by_month": {"partitions": []},
                "by_category": {"partitions": []}
            },
            "top_products": {
                "by_category": {"partitions": ["product_category"]},
                "overall": {"partitions": []}
            },
            "user_retention": {
                "user_activity_dates": {"partitions": []},
                "user_monthly_activity": {"partitions": []}
            }
        }
    }
}

def get_spark_session():
    """Initialize and return a Spark session."""
    return (SparkSession.builder
            .appName("E-commerce Data Storage")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", "../data/warehouse")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.parquet.mergeSchema", "true")
            .config("spark.sql.parquet.filterPushdown", "true")
            .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
            .config("spark.sql.files.openCostInBytes", "4194304")  # 4 MB
            .master("local[*]")
            .getOrCreate())

def create_storage_structure():
    """Create the lakehouse storage structure."""
    logger.info("Creating lakehouse storage structure")
    
    base_dir = "../data"
    
    # Create base directories
    for layer in STORAGE_STRUCTURE.keys():
        layer_dir = os.path.join(base_dir, layer)
        os.makedirs(layer_dir, exist_ok=True)
        logger.info(f"Created {layer} layer directory: {layer_dir}")
        
        # Create subdirectories for each layer
        for entity, config in STORAGE_STRUCTURE[layer].items():
            if isinstance(config, dict):
                # Simple entity
                entity_dir = os.path.join(layer_dir, entity)
                os.makedirs(entity_dir, exist_ok=True)
                logger.info(f"Created {layer}/{entity} directory: {entity_dir}")
            else:
                # Nested structure
                for sub_entity, sub_config in config.items():
                    sub_entity_dir = os.path.join(layer_dir, entity, sub_entity)
                    os.makedirs(sub_entity_dir, exist_ok=True)
                    logger.info(f"Created {layer}/{entity}/{sub_entity} directory: {sub_entity_dir}")
                    
                    # Handle nested structures (like KPIs)
                    if isinstance(sub_config, dict) and any(isinstance(v, dict) for v in sub_config.values()):
                        for sub_sub_entity in sub_config.keys():
                            sub_sub_entity_dir = os.path.join(sub_entity_dir, sub_sub_entity)
                            os.makedirs(sub_sub_entity_dir, exist_ok=True)
                            logger.info(f"Created {layer}/{entity}/{sub_entity}/{sub_sub_entity} directory: {sub_sub_entity_dir}")
    
    # Create checkpoint directory
    checkpoint_dir = os.path.join(base_dir, "checkpoints")
    os.makedirs(checkpoint_dir, exist_ok=True)
    logger.info(f"Created checkpoint directory: {checkpoint_dir}")
    
    # Create warehouse directory
    warehouse_dir = os.path.join(base_dir, "warehouse")
    os.makedirs(warehouse_dir, exist_ok=True)
    logger.info(f"Created warehouse directory: {warehouse_dir}")
    
    logger.info("Lakehouse storage structure created successfully")

def optimize_parquet_storage(spark, input_path, output_path, partition_columns=None):
    """
    Optimize Parquet storage by repartitioning and compacting files.
    
    Args:
        spark: SparkSession
        input_path: Path to input Parquet directory
        output_path: Path to output Parquet directory
        partition_columns: List of columns to partition by
    """
    logger.info(f"Optimizing Parquet storage for {input_path}")
    
    try:
        # Read Parquet files
        df = spark.read.parquet(input_path)
        
        # Repartition to optimize file size
        num_partitions = max(1, df.count() // 100000)  # Aim for ~100K rows per partition
        if partition_columns:
            df = df.repartition(num_partitions, *partition_columns)
        else:
            df = df.repartition(num_partitions)
        
        # Write optimized Parquet
        if partition_columns:
            df.write.mode("overwrite").partitionBy(*partition_columns).parquet(output_path)
        else:
            df.write.mode("overwrite").parquet(output_path)
        
        logger.info(f"Successfully optimized Parquet storage for {input_path}")
    
    except Exception as e:
        logger.error(f"Error optimizing Parquet storage: {str(e)}")
        raise

def analyze_storage_usage():
    """Analyze storage usage and generate a report."""
    logger.info("Analyzing storage usage")
    
    base_dir = "../data"
    report = []
    
    # Analyze each layer
    for layer in STORAGE_STRUCTURE.keys():
        layer_dir = os.path.join(base_dir, layer)
        layer_size = get_directory_size(layer_dir)
        report.append(f"{layer} layer: {format_size(layer_size)}")
        
        # Analyze entities in each layer
        for entity in os.listdir(layer_dir):
            entity_dir = os.path.join(layer_dir, entity)
            if os.path.isdir(entity_dir):
                entity_size = get_directory_size(entity_dir)
                report.append(f"  {entity}: {format_size(entity_size)}")
    
    # Write report to file
    report_file = os.path.join(base_dir, "storage_report.txt")
    with open(report_file, 'w') as f:
        f.write("Storage Usage Report\n")
        f.write("===================\n\n")
        f.write("\n".join(report))
    
    logger.info(f"Storage usage report written to {report_file}")
    return report

def get_directory_size(path):
    """Get the size of a directory in bytes."""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            total_size += os.path.getsize(file_path)
    return total_size

def format_size(size_bytes):
    """Format size in bytes to human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"

def implement_data_retention_policy():
    """Implement data retention policy for the lakehouse."""
    logger.info("Implementing data retention policy")
    
    # In a real scenario, we would implement a more sophisticated retention policy
    # For this demo, we'll just log a message
    logger.info("Data retention policy would delete old data based on business rules")
    logger.info("For this demo, all data is retained")

def main():
    """Main function to set up the data storage structure."""
    # Initialize Spark session
    spark = get_spark_session()
    
    try:
        # Create storage structure
        create_storage_structure()
        
        # Analyze storage usage
        storage_report = analyze_storage_usage()
        for line in storage_report:
            logger.info(line)
        
        # Implement data retention policy
        implement_data_retention_policy()
        
        logger.info("Data storage structure setup completed successfully")
    
    except Exception as e:
        logger.error(f"Error in data storage setup: {str(e)}")
    
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()
