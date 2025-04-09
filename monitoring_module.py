#!/usr/bin/env python3
"""
Monitoring Module for E-commerce Data Pipeline

This module implements monitoring and logging capabilities for the data pipeline.
It includes data quality monitoring, pipeline health checks, and alerting for failures and anomalies.

Features:
- Comprehensive logging system
- Data quality monitoring
- Pipeline health checks
- Anomaly detection
- Alerting system
"""

import os
import sys
import json
import time
import logging
import datetime
import statistics
from pathlib import Path
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, stddev, min as spark_min, max as spark_max

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/monitoring.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("monitoring")

# Create logs directory if it doesn't exist
os.makedirs("../logs", exist_ok=True)

# Create monitoring directory if it doesn't exist
os.makedirs("../monitoring", exist_ok=True)
os.makedirs("../monitoring/data_quality", exist_ok=True)
os.makedirs("../monitoring/pipeline_health", exist_ok=True)
os.makedirs("../monitoring/anomaly_detection", exist_ok=True)

def get_spark_session():
    """Initialize and return a Spark session."""
    return (SparkSession.builder
            .appName("E-commerce Data Monitoring")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", "../data/warehouse")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "2g")
            .master("local[*]")
            .getOrCreate())

class DataQualityMonitor:
    """Data Quality Monitoring for the data pipeline."""
    
    def __init__(self, spark, output_dir="../monitoring/data_quality"):
        """
        Initialize the data quality monitor.
        
        Args:
            spark: SparkSession
            output_dir: Directory to store monitoring results
        """
        self.spark = spark
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Initialized data quality monitor with output directory: {output_dir}")
    
    def check_completeness(self, df, column_name):
        """
        Check completeness (non-null values) of a column.
        
        Args:
            df: DataFrame to check
            column_name: Column to check
        
        Returns:
            Completeness ratio (0-1)
        """
        total_count = df.count()
        non_null_count = df.filter(col(column_name).isNotNull()).count()
        completeness = non_null_count / total_count if total_count > 0 else 0
        
        return completeness
    
    def check_uniqueness(self, df, column_name):
        """
        Check uniqueness of values in a column.
        
        Args:
            df: DataFrame to check
            column_name: Column to check
        
        Returns:
            Uniqueness ratio (0-1)
        """
        total_count = df.count()
        unique_count = df.select(column_name).distinct().count()
        uniqueness = unique_count / total_count if total_count > 0 else 0
        
        return uniqueness
    
    def check_value_distribution(self, df, column_name):
        """
        Check value distribution of a column.
        
        Args:
            df: DataFrame to check
            column_name: Column to check
        
        Returns:
            Dictionary with distribution statistics
        """
        # Get column statistics
        stats = df.select(
            spark_min(column_name).alias("min"),
            spark_max(column_name).alias("max"),
            avg(column_name).alias("mean"),
            stddev(column_name).alias("stddev")
        ).collect()[0]
        
        # Get value counts
        value_counts = df.groupBy(column_name).count().orderBy("count", ascending=False)
        
        # Convert to dictionary
        distribution = {
            "min": stats["min"],
            "max": stats["max"],
            "mean": stats["mean"],
            "stddev": stats["stddev"],
            "top_values": [
                {"value": row[column_name], "count": row["count"]}
                for row in value_counts.limit(10).collect()
            ]
        }
        
        return distribution
    
    def check_referential_integrity(self, df, column_name, ref_df, ref_column_name):
        """
        Check referential integrity between two DataFrames.
        
        Args:
            df: DataFrame to check
            column_name: Column to check
            ref_df: Reference DataFrame
            ref_column_name: Reference column
        
        Returns:
            Integrity ratio (0-1)
        """
        # Get distinct values from both DataFrames
        values = df.select(column_name).distinct()
        ref_values = ref_df.select(ref_column_name).distinct()
        
        # Count values that exist in reference
        total_count = values.count()
        matching_count = values.join(
            ref_values,
            values[column_name] == ref_values[ref_column_name],
            "inner"
        ).count()
        
        integrity = matching_count / total_count if total_count > 0 else 0
        
        return integrity
    
    def run_data_quality_checks(self, dataset_name, df, key_columns, numeric_columns=None, categorical_columns=None, reference_checks=None):
        """
        Run data quality checks on a DataFrame.
        
        Args:
            dataset_name: Name of the dataset
            df: DataFrame to check
            key_columns: List of key columns to check uniqueness
            numeric_columns: List of numeric columns to check distribution
            categorical_columns: List of categorical columns to check distribution
            reference_checks: List of reference checks (dict with src_col, ref_df, ref_col)
        
        Returns:
            Dictionary with data quality results
        """
        logger.info(f"Running data quality checks for {dataset_name}")
        
        results = {
            "dataset_name": dataset_name,
            "timestamp": datetime.datetime.now().isoformat(),
            "row_count": df.count(),
            "column_count": len(df.columns),
            "completeness": {},
            "uniqueness": {},
            "distributions": {},
            "referential_integrity": {}
        }
        
        # Check completeness for all columns
        for column in df.columns:
            results["completeness"][column] = self.check_completeness(df, column)
        
        # Check uniqueness for key columns
        for column in key_columns:
            results["uniqueness"][column] = self.check_uniqueness(df, column)
        
        # Check distributions for numeric columns
        if numeric_columns:
            for column in numeric_columns:
                if column in df.columns:
                    results["distributions"][column] = self.check_value_distribution(df, column)
        
        # Check distributions for categorical columns
        if categorical_columns:
            for column in categorical_columns:
                if column in df.columns:
                    results["distributions"][column] = self.check_value_distribution(df, column)
        
        # Check referential integrity
        if reference_checks:
            for check in reference_checks:
                src_col = check["src_col"]
                ref_df = check["ref_df"]
                ref_col = check["ref_col"]
                
                results["referential_integrity"][f"{src_col}_to_{ref_col}"] = self.check_referential_integrity(
                    df, src_col, ref_df, ref_col
                )
        
        # Save results
        output_file = os.path.join(self.output_dir, f"{dataset_name}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json")
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"Data quality results saved to {output_file}")
        return results
    
    def detect_anomalies(self, dataset_name, df, column_name, method="zscore", threshold=3.0):
        """
        Detect anomalies in a column.
        
        Args:
            dataset_name: Name of the dataset
            df: DataFrame to check
            column_name: Column to check
            method: Anomaly detection method (zscore, iqr)
            threshold: Threshold for anomaly detection
        
        Returns:
            DataFrame with anomalies
        """
        logger.info(f"Detecting anomalies in {dataset_name}.{column_name} using {method} method")
        
        if method == "zscore":
            # Calculate Z-score
            stats = df.select(
                avg(column_name).alias("mean"),
                stddev(column_name).alias("stddev")
            ).collect()[0]
            
            mean = stats["mean"]
            stddev = stats["stddev"]
            
            # Detect anomalies
            anomalies = df.filter(
                (col(column_name) > mean + threshold * stddev) |
                (col(column_name) < mean - threshold * stddev)
            )
        
        elif method == "iqr":
            # Calculate IQR
            quantiles = df.approxQuantile(column_name, [0.25, 0.75], 0.05)
            q1 = quantiles[0]
            q3 = quantiles[1]
            iqr = q3 - q1
            
            # Detect anomalies
            anomalies = df.filter(
                (col(column_name) > q3 + threshold * iqr) |
                (col(column_name) < q1 - threshold * iqr)
            )
        
        else:
            raise ValueError(f"Unknown anomaly detection method: {method}")
        
        # Save results
        output_file = os.path.join(self.output_dir, f"{dataset_name}_{column_name}_anomalies_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json")
        
        # Convert to pandas for easier serialization
        anomalies_pd = anomalies.toPandas()
        anomalies_pd.to_json(output_file, orient="records", lines=True)
        
        logger.info(f"Detected {anomalies.count()} anomalies in {dataset_name}.{column_name}")
        logger.info(f"Anomalies saved to {output_file}")
        
        return anomalies

class PipelineHealthMonitor:
    """Pipeline Health Monitoring for the data pipeline."""
    
    def __init__(self, output_dir="../monitoring/pipeline_health"):
        """
        Initialize the pipeline health monitor.
        
        Args:
            output_dir: Directory to store monitoring results
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Initialized pipeline health monitor with output directory: {output_dir}")
    
    def check_directory_size(self, directory):
        """
        Check the size of a directory.
        
        Args:
            directory: Directory to check
        
        Returns:
            Size in bytes
        """
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(directory):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                total_size += os.path.getsize(file_path)
        
        return total_size
    
    def check_file_count(self, directory, extension=None):
        """
        Check the number of files in a directory.
        
        Args:
            directory: Directory to check
            extension: File extension to filter (optional)
        
        Returns:
            Number of files
        """
        count = 0
        for dirpath, dirnames, filenames in os.walk(directory):
            for filename in filenames:
                if extension is None or filename.endswith(extension):
                    count += 1
        
        return count
    
    def check_processing_time(self, log_file, task_name):
        """
        Check the processing time of a task from log file.
        
        Args:
            log_file: Log file to check
            task_name: Task name to filter
        
        Returns:
            Processing time in seconds
        """
        start_time = None
        end_time = None
        
        with open(log_file, 'r') as f:
            for line in f:
                if task_name in line and "Starting" in line:
                    # Extract timestamp
                    timestamp_str = line.split(" - ")[0]
                    start_time = datetime.datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
                
                if task_name in line and "Completed" in line:
                    # Extract timestamp
                    timestamp_str = line.split(" - ")[0]
                    end_time = datetime.datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
        
        if start_time and end_time:
            return (end_time - start_time).total_seconds()
        else:
            return None
    
    def check_error_count(self, log_file, level="ERROR"):
        """
        Check the number of errors in a log file.
        
        Args:
            log_file: Log file to check
            level: Log level to filter
        
        Returns:
            Number of errors
        """
        count = 0
        
        with open(log_file, 'r') as f:
            for line in f:
                if f" - {level} - " in line:
                    count += 1
        
        return count
    
    def run_pipeline_health_checks(self):
        """
        Run pipeline health checks.
        
        Returns:
            Dictionary with pipeline health results
        """
        logger.info("Running pipeline health checks")
        
        results = {
            "timestamp": datetime.datetime.now().isoformat(),
            "directory_sizes": {},
            "file_counts": {},
            "processing_times": {},
            "error_counts": {}
        }
        
        # Check directory sizes
        for layer in ["bronze", "silver", "gold"]:
            layer_dir = f"../data/{layer}"
            if os.path.exists(layer_dir):
                results["directory_sizes"][layer] = self.check_directory_size(layer_dir)
        
        # Check file counts
        for layer in ["bronze", "silver", "gold"]:
            layer_dir = f"../data/{layer}"
            if os.path.exists(layer_dir):
                results["file_counts"][layer] = {
                    "total": self.check_file_count(layer_dir),
                    "parquet": self.check_file_count(layer_dir, ".parquet")
                }
        
        # Check processing times
        log_files = {
            "ingestion": "../logs/ingestion.log",
            "processing": "../logs/processing.log",
            "storage": "../logs/storage.log",
            "orchestration": "../logs/orchestration.log"
        }
        
        for log_name, log_file in log_files.items():
            if os.path.exists(log_file):
                results["processing_times"][log_name] = {}
                
                # Check different tasks based on log file
                if log_name == "ingestion":
                    tasks = ["ingest_user_activity", "ingest_orders", "ingest_inventory"]
                elif log_name == "processing":
                    tasks = ["clean_user_activity", "clean_orders", "clean_inventory", "transform_to_gold"]
                elif log_name == "storage":
                    tasks = ["optimize_storage"]
                elif log_name == "orchestration":
                    tasks = ["daily_processing_flow", "weekly_processing_flow"]
                
                for task in tasks:
        
(Content truncated due to size limit. Use line ranges to read in chunks)