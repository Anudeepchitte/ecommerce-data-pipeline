#!/usr/bin/env python3
"""
Gold Layer Validation Module for E-commerce Data Pipeline

This module implements data validation for the gold layer (business-ready data) using Great Expectations.
It defines expectations for dimension tables, fact tables, and KPI data with the most stringent
quality checks in the data pipeline.

Features:
- Business rule validation for gold layer data
- Dimension and fact table validation
- KPI data validation
- Distribution anomaly detection
- Cross-table relationship validation
"""

import os
import sys
import logging
import json
import pandas as pd
import numpy as np
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.validator.validator import Validator
from pyspark.sql import SparkSession

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import GE setup module
from data_validation.ge_setup import create_data_context, configure_spark_datasource

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/gold_validation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("gold_validation")

# Create logs directory if it doesn't exist
os.makedirs("../logs", exist_ok=True)

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
GOLD_DIR = os.path.join(DATA_DIR, "gold")

def get_spark_session():
    """Initialize and return a Spark session."""
    return (SparkSession.builder
            .appName("E-commerce Gold Data Validation")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", os.path.join(DATA_DIR, "warehouse"))
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "2g")
            .master("local[*]")
            .getOrCreate())

def create_dim_customer_expectations(context, suite_name="dim_customer_gold_suite"):
    """
    Create expectations for customer dimension table in the gold layer.
    
    Args:
        context: Great Expectations data context
        suite_name: Name of the expectation suite
    
    Returns:
        ExpectationSuite: Created expectation suite
    """
    logger.info(f"Creating expectations for customer dimension table: {suite_name}")
    
    try:
        # Create or get expectation suite
        suite = context.create_expectation_suite(
            expectation_suite_name=suite_name,
            overwrite_existing=True
        )
        
        # Get validator
        spark = get_spark_session()
        
        # Load sample data if available, otherwise create empty DataFrame with expected schema
        try:
            df = spark.read.parquet(os.path.join(GOLD_DIR, "dimensions/dim_customer"))
        except Exception:
            # Create empty DataFrame with expected schema if data doesn't exist
            schema = [
                ("customer_id", "string"),
                ("customer_since_date", "date"),
                ("last_order_date", "date"),
                ("total_orders", "integer"),
                ("total_spend", "double"),
                ("avg_order_value", "double"),
                ("preferred_payment_method", "string"),
                ("preferred_shipping_method", "string"),
                ("country", "string"),
                ("city", "string"),
                ("is_active", "boolean")
            ]
            
            data = []
            df = spark.createDataFrame(data, schema=[f"{name} {dtype}" for name, dtype in schema])
        
        # Create batch request
        batch_request = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="default_runtime_data_connector",
            data_asset_name="dim_customer_gold",
            batch_identifiers={
                "batch_id": "default_batch",
                "layer": "gold",
                "dataset": "dim_customer"
            },
            runtime_parameters={"batch_data": df},
        )
        
        # Get validator
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name
        )
        
        # Add expectations
        
        # Table level expectations
        validator.expect_table_row_count_to_be_between(min_value=1, max_value=None)
        validator.expect_table_columns_to_match_ordered_list(
            column_list=[
                "customer_id", "customer_since_date", "last_order_date", "total_orders",
                "total_spend", "avg_order_value", "preferred_payment_method", 
                "preferred_shipping_method", "country", "city", "is_active"
            ]
        )
        
        # Column level expectations - customer_id
        validator.expect_column_to_exist("customer_id")
        validator.expect_column_values_to_not_be_null("customer_id")
        validator.expect_column_values_to_match_regex("customer_id", r"^user_\d+$")
        validator.expect_column_values_to_be_unique("customer_id")
        
        # Column level expectations - customer_since_date
        validator.expect_column_to_exist("customer_since_date")
        validator.expect_column_values_to_not_be_null("customer_since_date")
        validator.expect_column_values_to_be_of_type("customer_since_date", "DateType")
        
        # Column level expectations - last_order_date
        validator.expect_column_to_exist("last_order_date")
        validator.expect_column_values_to_not_be_null("last_order_date")
        validator.expect_column_values_to_be_of_type("last_order_date", "DateType")
        
        # Column level expectations - total_orders
        validator.expect_column_to_exist("total_orders")
        validator.expect_column_values_to_not_be_null("total_orders")
        validator.expect_column_values_to_be_of_type("total_orders", "IntegerType")
        validator.expect_column_values_to_be_between("total_orders", min_value=0, max_value=None)
        
        # Column level expectations - total_spend
        validator.expect_column_to_exist("total_spend")
        validator.expect_column_values_to_not_be_null("total_spend")
        validator.expect_column_values_to_be_of_type("total_spend", "DoubleType")
        validator.expect_column_values_to_be_between("total_spend", min_value=0, max_value=None)
        
        # Column level expectations - avg_order_value
        validator.expect_column_to_exist("avg_order_value")
        validator.expect_column_values_to_not_be_null("avg_order_value")
        validator.expect_column_values_to_be_of_type("avg_order_value", "DoubleType")
        validator.expect_column_values_to_be_between("avg_order_value", min_value=0, max_value=None)
        
        # Column level expectations - preferred_payment_method
        validator.expect_column_to_exist("preferred_payment_method")
        validator.expect_column_values_to_not_be_null("preferred_payment_method", mostly=0.95)  # Allow some nulls
        validator.expect_column_values_to_be_in_set(
            "preferred_payment_method", 
            ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "gift_card", "bank_transfer", None]
        )
        
        # Column level expectations - preferred_shipping_method
        validator.expect_column_to_exist("preferred_shipping_method")
        validator.expect_column_values_to_not_be_null("preferred_shipping_method", mostly=0.95)  # Allow some nulls
        validator.expect_column_values_to_be_in_set(
            "preferred_shipping_method", 
            ["standard", "express", "next_day", "pickup", "free", None]
        )
        
        # Column level expectations - country
        validator.expect_column_to_exist("country")
        validator.expect_column_values_to_not_be_null("country")
        
        # Column level expectations - city
        validator.expect_column_to_exist("city")
        validator.expect_column_values_to_not_be_null("city", mostly=0.95)  # Allow some nulls
        
        # Column level expectations - is_active
        validator.expect_column_to_exist("is_active")
        validator.expect_column_values_to_not_be_null("is_active")
        validator.expect_column_values_to_be_of_type("is_active", "BooleanType")
        
        # Relationship expectations
        validator.expect_column_pair_values_to_be_equal(
            "total_spend",
            "avg_order_value",
            or_equal=None,
            row_condition="total_orders == 1",
            condition_parser="pandas"
        )
        
        validator.expect_column_values_to_be_between(
            "avg_order_value",
            min_value=0,
            max_value=None,
            row_condition="total_orders > 0",
            condition_parser="pandas"
        )
        
        # Distribution expectations
        validator.expect_column_quantile_values_to_be_between(
            "total_spend",
            quantile_ranges={
                "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                "value_ranges": [
                    [0, None],  # 5th percentile
                    [0, None],  # 25th percentile
                    [0, None],  # 50th percentile
                    [0, None],  # 75th percentile
                    [0, None]   # 95th percentile
                ]
            }
        )
        
        # Save expectation suite
        validator.save_expectation_suite(discard_failed_expectations=False)
        
        logger.info(f"Expectation suite '{suite_name}' created successfully")
        return suite
    
    except Exception as e:
        logger.error(f"Error creating expectations for customer dimension table: {str(e)}")
        raise

def create_dim_product_expectations(context, suite_name="dim_product_gold_suite"):
    """
    Create expectations for product dimension table in the gold layer.
    
    Args:
        context: Great Expectations data context
        suite_name: Name of the expectation suite
    
    Returns:
        ExpectationSuite: Created expectation suite
    """
    logger.info(f"Creating expectations for product dimension table: {suite_name}")
    
    try:
        # Create or get expectation suite
        suite = context.create_expectation_suite(
            expectation_suite_name=suite_name,
            overwrite_existing=True
        )
        
        # Get validator
        spark = get_spark_session()
        
        # Load sample data if available, otherwise create empty DataFrame with expected schema
        try:
            df = spark.read.parquet(os.path.join(GOLD_DIR, "dimensions/dim_product"))
        except Exception:
            # Create empty DataFrame with expected schema if data doesn't exist
            schema = [
                ("product_id", "integer"),
                ("product_name", "string"),
                ("category", "string"),
                ("subcategory", "string"),
                ("brand", "string"),
                ("supplier", "string"),
                ("current_retail_price", "double"),
                ("current_cost_price", "double"),
                ("profit_margin", "double"),
                ("current_stock", "integer"),
                ("is_active", "boolean"),
                ("first_available_date", "date"),
                ("last_restock_date", "date")
            ]
            
            data = []
            df = spark.createDataFrame(data, schema=[f"{name} {dtype}" for name, dtype in schema])
        
        # Create batch request
        batch_request = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="default_runtime_data_connector",
            data_asset_name="dim_product_gold",
            batch_identifiers={
                "batch_id": "default_batch",
                "layer": "gold",
                "dataset": "dim_product"
            },
            runtime_parameters={"batch_data": df},
        )
        
        # Get validator
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name
        )
        
        # Add expectations
        
        # Table level expectations
        validator.expect_table_row_count_to_be_between(min_value=1, max_value=None)
        validator.expect_table_columns_to_match_ordered_list(
            column_list=[
                "product_id", "product_name", "category", "subcategory", "brand", "supplier",
                "current_retail_price", "current_cost_price", "profit_margin", "current_stock",
                "is_active", "first_available_date", "last_restock_date"
            ]
        )
        
        # Column level expectations - product_id
        validator.expect_column_to_exist("product_id")
        validator.expect_column_values_to_not_be_null("product_id")
        validator.expect_column_values_to_be_of_type("product_id", "IntegerType")
        validator.expect_column_values_to_be_unique("product_id")
        
        # Column level expectations - product_name
        validator.expect_column_to_exist("product_name")
        validator.expect_column_values_to_not_be_null("product_name")
        validator.expect_column_values_to_be_of_type("product_name", "StringType")
        
        # Column level expectations - category
        validator.expect_column_to_exist("category")
        validator.expect_column_values_to_not_be_null("category")
        validator.expect_column_values_to_be_of_type("category", "StringType")
        
        # Column level expectations - subcategory
        validator.expect_column_to_exist("subcategory")
        validator.expect_column_values_to_not_be_null("subcategory", mostly=0.95)  # Allow some nulls
        validator.expect_column_values_to_be_of_type("subcategory", "StringType")
        
        # Column level expectations - brand
        validator.expect_column_to_exist("brand")
        validator.expect_column_values_to_not_be_null("brand")
        validator.expect_column_values_to_be_of_type("brand", "StringType")
        
        # Column level expectations - supplier
        validator.expect_column_to_exist("supplier")
        validator.expect_column_values_to_not_be_null("supplier")
        validator.expect_column_values_to_be_of_type("supplier", "StringType")
        
        # Column level expectations - current_retail_price
        validator.expect_column_to_exist("current_retail_price")
        validator.expect_column_values_to_not_be_null("current_retail_price")
        validator.expect_column_values_to_be_of_type("current_retail_price", "DoubleType")
        validator.expect_column_values_to_be_between("current_retail_price", min_value=0, max_value=None)
        
        # Column level expectations - current_cost_price
        validator.expect_column_to_exist("current_cost_price")
        validator.expect_column_values_to_not_be_null("current_cost_price")
        validator.expect_column_values_to_be_of_type("current_cost_price", "DoubleType")
        validator.expect_column_values_to_be_between("current_cost_price", min_value=0, max_value=None)
        
        # Column level expectations - profit_margin
        validator.expect_column_to_exist("profit_margin")
        validator.expect_column_values_to_not_be_null("profit_margin")
        validator.expect_column_values_to_be_of_type("profit_margin", "DoubleType")
        validator.expect_column_values_to_be_between("profit_margin", min_value=0, max_value=1)
        
        # Column level expectations - current_stock
        validator.expect_column_to_exist("current_stock")
        validator.expect_column_values_to_not_be_null("current_stock")
        
(Content truncated due to size limit. Use line ranges to read in chunks)