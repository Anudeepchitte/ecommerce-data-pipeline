#!/usr/bin/env python3
"""
Performance Optimization Module for E-commerce Data Pipeline Validation

This module implements optimization strategies for data validation to improve
performance and resource efficiency. It includes selective validation, sampling,
caching, and parallel processing techniques.

Features:
- Resource-efficient validation strategies
- Selective validation based on data changes
- Sampling techniques for large datasets
- Validation result caching
- Parallel validation execution
"""

import os
import sys
import logging
import json
import datetime
import hashlib
import pickle
from functools import lru_cache
import pandas as pd
import numpy as np
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.validator.validator import Validator
from pyspark.sql import SparkSession
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import validation modules
from data_validation.ge_setup import create_data_context, configure_spark_datasource

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/validation_optimization.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("validation_optimization")

# Create logs directory if it doesn't exist
os.makedirs("../logs", exist_ok=True)

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(BASE_DIR, "data_validation", "config")
CACHE_DIR = os.path.join(BASE_DIR, "data_validation", "cache")

# Create directories if they don't exist
os.makedirs(CONFIG_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# Default configuration
DEFAULT_CONFIG = {
    "optimization": {
        "enabled": True,
        "selective_validation": {
            "enabled": True,
            "check_data_hash": True,
            "check_schema_changes": True,
            "check_row_count_changes": True,
            "row_count_threshold": 0.05,  # 5% change threshold
            "skip_unchanged_data": True
        },
        "sampling": {
            "enabled": True,
            "threshold_rows": 1000000,  # Apply sampling for datasets larger than this
            "sampling_method": "random",  # random, systematic, or stratified
            "sample_size": 0.1,  # 10% sample size
            "min_sample_rows": 100000,  # Minimum sample size
            "stratify_columns": []  # Columns to stratify by (for stratified sampling)
        },
        "caching": {
            "enabled": True,
            "ttl_seconds": 3600,  # Cache time-to-live in seconds
            "max_cache_entries": 100,
            "cache_expectations": True,
            "cache_validation_results": True
        },
        "parallel_processing": {
            "enabled": True,
            "max_workers": 4,  # Maximum number of parallel workers
            "execution_method": "thread",  # thread or process
            "chunk_size": 100000  # Rows per chunk for parallel processing
        },
        "resource_limits": {
            "max_memory_mb": 4096,  # Maximum memory usage in MB
            "max_cpu_percent": 70,  # Maximum CPU usage percentage
            "timeout_seconds": 3600  # Maximum validation runtime in seconds
        },
        "lightweight_validation": {
            "enabled": True,
            "critical_expectations_only": False,
            "skip_expensive_expectations": True,
            "expensive_expectation_types": [
                "expect_column_pair_values_to_be_equal",
                "expect_column_values_to_match_regex",
                "expect_column_values_to_be_in_set",
                "expect_column_values_to_match_strftime_format",
                "expect_column_values_to_be_json_parseable",
                "expect_column_values_to_match_json_schema",
                "expect_column_values_to_be_xml_parseable",
                "expect_column_values_to_match_like_pattern",
                "expect_column_values_to_not_match_like_pattern",
                "expect_column_values_to_match_like_pattern_list",
                "expect_column_values_to_not_match_like_pattern_list"
            ]
        }
    }
}

class ValidationOptimizer:
    """Class for optimizing data validation performance."""
    
    def __init__(self, config_path=None):
        """
        Initialize the ValidationOptimizer.
        
        Args:
            config_path: Path to configuration file (optional)
        """
        self.config = self._load_config(config_path)
        self.cache = {}
        self.data_hashes = {}
        self.schema_hashes = {}
        self.row_counts = {}
        
        # Initialize resource monitoring
        self.start_time = None
        self.memory_usage = []
        self.cpu_usage = []
    
    def _load_config(self, config_path=None):
        """
        Load configuration from file or use default.
        
        Args:
            config_path: Path to configuration file (optional)
        
        Returns:
            dict: Configuration dictionary
        """
        if config_path is None:
            config_path = os.path.join(CONFIG_DIR, "optimization_config.json")
        
        # If config file doesn't exist, create it with default config
        if not os.path.exists(config_path):
            os.makedirs(os.path.dirname(config_path), exist_ok=True)
            with open(config_path, "w") as f:
                json.dump(DEFAULT_CONFIG, f, indent=2)
            logger.info(f"Created default configuration at: {config_path}")
            return DEFAULT_CONFIG
        
        # Load config from file
        try:
            with open(config_path, "r") as f:
                config = json.load(f)
            logger.info(f"Loaded configuration from: {config_path}")
            return config
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            logger.info("Using default configuration")
            return DEFAULT_CONFIG
    
    def _compute_data_hash(self, df, sample_size=1000):
        """
        Compute a hash of the data to detect changes.
        
        Args:
            df: DataFrame to hash
            sample_size: Number of rows to sample for hashing
        
        Returns:
            str: Hash of the data
        """
        try:
            # Sample data for faster hashing
            if df.count() > sample_size:
                sampled_df = df.sample(False, sample_size / df.count())
            else:
                sampled_df = df
            
            # Convert to pandas for hashing
            pdf = sampled_df.toPandas()
            
            # Sort by all columns to ensure consistent hashing
            pdf = pdf.sort_values(by=list(pdf.columns))
            
            # Compute hash of the data
            data_str = pdf.to_csv(index=False)
            return hashlib.md5(data_str.encode()).hexdigest()
        except Exception as e:
            logger.error(f"Error computing data hash: {str(e)}")
            return None
    
    def _compute_schema_hash(self, df):
        """
        Compute a hash of the schema to detect changes.
        
        Args:
            df: DataFrame to hash schema
        
        Returns:
            str: Hash of the schema
        """
        try:
            # Get schema as string
            schema_str = str(df.schema)
            
            # Compute hash of the schema
            return hashlib.md5(schema_str.encode()).hexdigest()
        except Exception as e:
            logger.error(f"Error computing schema hash: {str(e)}")
            return None
    
    def _should_validate(self, df, layer, dataset):
        """
        Determine if validation should be performed based on data changes.
        
        Args:
            df: DataFrame to validate
            layer: Data layer (bronze, silver, gold)
            dataset: Dataset name
        
        Returns:
            bool: Whether validation should be performed
        """
        if not self.config["optimization"]["selective_validation"]["enabled"]:
            return True
        
        key = f"{layer}_{dataset}"
        current_row_count = df.count()
        
        # Check row count changes
        if self.config["optimization"]["selective_validation"]["check_row_count_changes"]:
            if key in self.row_counts:
                previous_row_count = self.row_counts[key]
                row_count_threshold = self.config["optimization"]["selective_validation"]["row_count_threshold"]
                
                # Calculate percentage change
                if previous_row_count > 0:
                    change_percent = abs(current_row_count - previous_row_count) / previous_row_count
                    
                    if change_percent <= row_count_threshold:
                        logger.info(f"Row count change ({change_percent:.2%}) below threshold for {key}")
                    else:
                        logger.info(f"Row count change ({change_percent:.2%}) above threshold for {key}, validation required")
                        return True
                else:
                    # If previous row count was 0, always validate
                    return True
        
        # Check schema changes
        if self.config["optimization"]["selective_validation"]["check_schema_changes"]:
            current_schema_hash = self._compute_schema_hash(df)
            
            if key in self.schema_hashes:
                previous_schema_hash = self.schema_hashes[key]
                
                if current_schema_hash != previous_schema_hash:
                    logger.info(f"Schema changed for {key}, validation required")
                    return True
        
        # Check data hash changes
        if self.config["optimization"]["selective_validation"]["check_data_hash"]:
            current_data_hash = self._compute_data_hash(df)
            
            if key in self.data_hashes:
                previous_data_hash = self.data_hashes[key]
                
                if current_data_hash != previous_data_hash:
                    logger.info(f"Data changed for {key}, validation required")
                    return True
        
        # Skip validation if data hasn't changed
        if self.config["optimization"]["selective_validation"]["skip_unchanged_data"]:
            logger.info(f"Data unchanged for {key}, skipping validation")
            return False
        
        return True
    
    def _update_data_metadata(self, df, layer, dataset):
        """
        Update data metadata for future change detection.
        
        Args:
            df: DataFrame
            layer: Data layer (bronze, silver, gold)
            dataset: Dataset name
        """
        key = f"{layer}_{dataset}"
        
        # Update row count
        self.row_counts[key] = df.count()
        
        # Update schema hash
        self.schema_hashes[key] = self._compute_schema_hash(df)
        
        # Update data hash
        self.data_hashes[key] = self._compute_data_hash(df)
    
    def _apply_sampling(self, df):
        """
        Apply sampling to large datasets for faster validation.
        
        Args:
            df: DataFrame to sample
        
        Returns:
            DataFrame: Sampled DataFrame
        """
        if not self.config["optimization"]["sampling"]["enabled"]:
            return df
        
        row_count = df.count()
        threshold_rows = self.config["optimization"]["sampling"]["threshold_rows"]
        
        if row_count <= threshold_rows:
            logger.info(f"Dataset size ({row_count} rows) below sampling threshold, using full dataset")
            return df
        
        sampling_method = self.config["optimization"]["sampling"]["sampling_method"]
        sample_size = self.config["optimization"]["sampling"]["sample_size"]
        min_sample_rows = self.config["optimization"]["sampling"]["min_sample_rows"]
        
        # Calculate actual sample size
        actual_sample_size = max(min_sample_rows, int(row_count * sample_size))
        actual_fraction = min(1.0, actual_sample_size / row_count)
        
        logger.info(f"Applying {sampling_method} sampling with fraction {actual_fraction:.2%}")
        
        if sampling_method == "random":
            return df.sample(False, actual_fraction)
        elif sampling_method == "systematic":
            # Systematic sampling using row number
            df = df.withColumn("row_num", F.monotonically_increasing_id())
            step = int(1 / actual_fraction)
            return df.filter(F.col("row_num") % step == 0).drop("row_num")
        elif sampling_method == "stratified":
            # Stratified sampling using specified columns
            stratify_columns = self.config["optimization"]["sampling"]["stratify_columns"]
            
            if not stratify_columns:
                logger.warning("No stratify columns specified, falling back to random sampling")
                return df.sample(False, actual_fraction)
            
            # Ensure all stratify columns exist
            existing_columns = set(df.columns)
            valid_stratify_columns = [col for col in stratify_columns if col in existing_columns]
            
            if not valid_stratify_columns:
                logger.warning("No valid stratify columns found, falling back to random sampling")
                return df.sample(False, actual_fraction)
            
            # Perform stratified sampling
            fractions = {}
            for group in df.groupBy(valid_stratify_columns).count().collect():
                group_key = tuple(group[col] for col in valid_stratify_columns)
                fractions[group_key] = min(1.0, actual_sample_size / row_count)
            
            return df.sampleBy(valid_stratify_columns, fractions, seed=42)
        else:
            logger.warning(f"Unknown sampling method: {sampling_method}, using random sampling")
            return df.sample(False, actual_fraction)
    
    def _get_cached_result(self, layer, dataset, expectation_suite_name):
        """
        Get cached validation result if available.
        
        Args:
            layer: Data layer (bronze, silver, gold)
            dataset: Dataset name
            expectation_suite_name: Name of the expectation suite
        
        Returns:
            ValidationResult or None: Cached validation result or None if not found
        """
        if not self.config["optimization"]["caching"]["enabled"]:
            return None
        
        key = f"{layer}_{dataset}_{expectation_suite_name}"
        
        if key not in self.cache:
            return None
        
        cache_entry = self.cache[key]
        ttl_seconds = self.config["optimization"]["caching"]["ttl_seconds"]
        
        # Check if cache entry is still valid
        if datetime.datetime.now().timestamp() - cache_entry["timestamp"] > ttl_seconds:
            logger.info(f"Cache entry for {key} expired")
            return None
        
        logger.info(f"Using cached validation result for {key}")
        return cache_entry["result"]
    
    def _cache_validation_result(self, layer, dataset, expectation_suite_name, result):
        """
        Cache validation result for future use.
        
        Args:
            layer: Data layer (bronze, silver, gold)
            dataset: Dataset name
            expectation_suite_name: Name of the expectation suite
            result: Validation result to cache
        """
        if not self.config["optimization"]["caching"]["enabled"] or not self.config["optimization"]["caching"]["cache_validation_results"]:
            return
   
(Content truncated due to size limit. Use line ranges to read in chunks)