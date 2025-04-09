#!/usr/bin/env python3
"""
Data Storage Optimization Module for E-commerce Data Pipeline

This module provides utilities for optimizing data storage in the lakehouse model.
It includes functions for partitioning, compaction, and storage format optimization.

Features:
- Parquet optimization
- File compaction
- Partition optimization
- Storage statistics
"""

import os
import logging
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, max as spark_max

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/storage_optimization.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("storage_optimization")

# Create logs directory if it doesn't exist
os.makedirs("../logs", exist_ok=True)

def get_spark_session():
    """Initialize and return a Spark session."""
    return (SparkSession.builder
            .appName("E-commerce Data Storage Optimization")
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

def compact_parquet_files(spark, input_path, output_path, partition_columns=None, target_size_mb=128):
    """
    Compact small Parquet files into larger ones for better performance.
    
    Args:
        spark: SparkSession
        input_path: Path to input Parquet directory
        output_path: Path to output Parquet directory
        partition_columns: List of columns to partition by
        target_size_mb: Target size of each Parquet file in MB
    """
    logger.info(f"Compacting Parquet files in {input_path}")
    
    try:
        # Read Parquet files
        df = spark.read.parquet(input_path)
        
        # Get row count
        row_count = df.count()
        
        # Estimate size per row (assuming 1KB per row as a rough estimate)
        size_per_row_kb = 1
        
        # Calculate target number of partitions
        total_size_kb = row_count * size_per_row_kb
        target_size_kb = target_size_mb * 1024
        num_partitions = max(1, total_size_kb // target_size_kb)
        
        logger.info(f"Compacting {row_count} rows into approximately {num_partitions} partitions")
        
        # Repartition
        if partition_columns:
            df = df.repartition(num_partitions, *partition_columns)
        else:
            df = df.repartition(num_partitions)
        
        # Write compacted Parquet
        if partition_columns:
            df.write.mode("overwrite").partitionBy(*partition_columns).parquet(output_path)
        else:
            df.write.mode("overwrite").parquet(output_path)
        
        logger.info(f"Successfully compacted Parquet files in {input_path}")
    
    except Exception as e:
        logger.error(f"Error compacting Parquet files: {str(e)}")
        raise

def analyze_partition_skew(spark, input_path, partition_columns):
    """
    Analyze partition skew to identify imbalanced partitions.
    
    Args:
        spark: SparkSession
        input_path: Path to Parquet directory
        partition_columns: List of partition columns to analyze
    
    Returns:
        Dictionary with partition statistics
    """
    logger.info(f"Analyzing partition skew for {input_path}")
    
    try:
        # Read Parquet files
        df = spark.read.parquet(input_path)
        
        # Analyze each partition column
        stats = {}
        for column in partition_columns:
            # Count rows per partition value
            partition_counts = df.groupBy(column).agg(count("*").alias("count"))
            
            # Collect statistics
            partition_stats = partition_counts.agg(
                spark_max("count").alias("max_count"),
                spark_sum("count").alias("total_count")
            ).collect()[0]
            
            # Calculate skew ratio (max partition size / average partition size)
            distinct_values = partition_counts.count()
            avg_count = partition_stats["total_count"] / distinct_values if distinct_values > 0 else 0
            skew_ratio = partition_stats["max_count"] / avg_count if avg_count > 0 else 0
            
            stats[column] = {
                "distinct_values": distinct_values,
                "max_count": partition_stats["max_count"],
                "avg_count": avg_count,
                "skew_ratio": skew_ratio
            }
            
            logger.info(f"Partition column {column}: {stats[column]}")
        
        return stats
    
    except Exception as e:
        logger.error(f"Error analyzing partition skew: {str(e)}")
        raise

def optimize_partitioning(spark, input_path, output_path, partition_columns, skew_threshold=3.0):
    """
    Optimize partitioning based on skew analysis.
    
    Args:
        spark: SparkSession
        input_path: Path to input Parquet directory
        output_path: Path to output Parquet directory
        partition_columns: List of columns to partition by
        skew_threshold: Threshold for skew ratio to trigger optimization
    """
    logger.info(f"Optimizing partitioning for {input_path}")
    
    try:
        # Analyze partition skew
        skew_stats = analyze_partition_skew(spark, input_path, partition_columns)
        
        # Identify skewed columns
        skewed_columns = [
            column for column, stats in skew_stats.items()
            if stats["skew_ratio"] > skew_threshold
        ]
        
        if skewed_columns:
            logger.info(f"Found skewed columns: {skewed_columns}")
            
            # Read Parquet files
            df = spark.read.parquet(input_path)
            
            # For skewed columns, we can use bucketing or salting techniques
            # For this demo, we'll just exclude them from partitioning
            optimized_partition_columns = [
                column for column in partition_columns
                if column not in skewed_columns
            ]
            
            logger.info(f"Using optimized partition columns: {optimized_partition_columns}")
            
            # Write with optimized partitioning
            if optimized_partition_columns:
                df.write.mode("overwrite").partitionBy(*optimized_partition_columns).parquet(output_path)
            else:
                # If all columns are skewed, don't partition
                df.write.mode("overwrite").parquet(output_path)
        else:
            logger.info("No skewed columns found, using original partitioning")
            
            # Read and write with original partitioning
            df = spark.read.parquet(input_path)
            df.write.mode("overwrite").partitionBy(*partition_columns).parquet(output_path)
        
        logger.info(f"Successfully optimized partitioning for {input_path}")
    
    except Exception as e:
        logger.error(f"Error optimizing partitioning: {str(e)}")
        raise

def generate_storage_statistics(base_dir="../data"):
    """
    Generate storage statistics for the lakehouse.
    
    Args:
        base_dir: Base directory of the lakehouse
    
    Returns:
        Dictionary with storage statistics
    """
    logger.info(f"Generating storage statistics for {base_dir}")
    
    try:
        stats = {
            "total_size": 0,
            "layers": {},
            "file_counts": {
                "total": 0,
                "by_extension": {}
            },
            "timestamp": time.time()
        }
        
        # Analyze each layer
        for layer in ["bronze", "silver", "gold"]:
            layer_dir = os.path.join(base_dir, layer)
            if os.path.exists(layer_dir):
                layer_stats = analyze_directory(layer_dir)
                stats["layers"][layer] = layer_stats
                stats["total_size"] += layer_stats["size"]
                stats["file_counts"]["total"] += layer_stats["file_count"]
                
                # Aggregate file extensions
                for ext, count in layer_stats["file_extensions"].items():
                    if ext not in stats["file_counts"]["by_extension"]:
                        stats["file_counts"]["by_extension"][ext] = 0
                    stats["file_counts"]["by_extension"][ext] += count
        
        # Write statistics to file
        stats_file = os.path.join(base_dir, "storage_statistics.json")
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"Storage statistics written to {stats_file}")
        return stats
    
    except Exception as e:
        logger.error(f"Error generating storage statistics: {str(e)}")
        raise

def analyze_directory(directory):
    """
    Analyze a directory for storage statistics.
    
    Args:
        directory: Directory to analyze
    
    Returns:
        Dictionary with directory statistics
    """
    stats = {
        "size": 0,
        "file_count": 0,
        "subdirectories": {},
        "file_extensions": {}
    }
    
    # Walk directory
    for root, dirs, files in os.walk(directory):
        # Calculate size and count files
        for file in files:
            file_path = os.path.join(root, file)
            file_size = os.path.getsize(file_path)
            stats["size"] += file_size
            stats["file_count"] += 1
            
            # Count file extensions
            _, ext = os.path.splitext(file)
            ext = ext.lower()
            if ext not in stats["file_extensions"]:
                stats["file_extensions"][ext] = 0
            stats["file_extensions"][ext] += 1
        
        # Calculate subdirectory stats
        for dir_name in dirs:
            subdir = os.path.join(root, dir_name)
            rel_path = os.path.relpath(subdir, directory)
            if '/' not in rel_path:  # Only direct subdirectories
                subdir_stats = analyze_directory(subdir)
                stats["subdirectories"][dir_name] = subdir_stats
    
    return stats

def main():
    """Main function to run storage optimization."""
    # Initialize Spark session
    spark = get_spark_session()
    
    try:
        # Example: Compact Parquet files
        compact_parquet_files(
            spark,
            "../data/bronze/user_activity",
            "../data/bronze/user_activity_compacted",
            ["event_type"],
            128
        )
        
        # Example: Optimize partitioning
        optimize_partitioning(
            spark,
            "../data/silver/orders",
            "../data/silver/orders_optimized",
            ["order_year", "order_month", "order_status"],
            3.0
        )
        
        # Generate storage statistics
        stats = generate_storage_statistics()
        logger.info(f"Total storage size: {stats['total_size']} bytes")
        logger.info(f"Total file count: {stats['file_counts']['total']}")
        
        logger.info("Storage optimization completed successfully")
    
    except Exception as e:
        logger.error(f"Error in storage optimization: {str(e)}")
    
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()
