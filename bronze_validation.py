#!/usr/bin/env python3
"""
Bronze Layer Validation Module for E-commerce Data Pipeline

This module implements data validation for the bronze layer (raw data) using Great Expectations.
It defines expectations for user activity, orders, and inventory data.

Features:
- Schema validation for raw data
- Basic data quality checks for bronze layer
- Null value detection
- Data type validation
- Format validation for dates, IDs, etc.
"""

import os
import sys
import logging
import json
import pandas as pd
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
        logging.FileHandler("../logs/bronze_validation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("bronze_validation")

# Create logs directory if it doesn't exist
os.makedirs("../logs", exist_ok=True)

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")

def get_spark_session():
    """Initialize and return a Spark session."""
    return (SparkSession.builder
            .appName("E-commerce Bronze Data Validation")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", os.path.join(DATA_DIR, "warehouse"))
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "2g")
            .master("local[*]")
            .getOrCreate())

def create_user_activity_expectations(context, suite_name="user_activity_bronze_suite"):
    """
    Create expectations for user activity data in the bronze layer.
    
    Args:
        context: Great Expectations data context
        suite_name: Name of the expectation suite
    
    Returns:
        ExpectationSuite: Created expectation suite
    """
    logger.info(f"Creating expectations for user activity data: {suite_name}")
    
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
            df = spark.read.parquet(os.path.join(BRONZE_DIR, "user_activity"))
        except Exception:
            # Create empty DataFrame with expected schema if data doesn't exist
            schema = [
                ("user_id", "string"),
                ("session_id", "string"),
                ("timestamp", "string"),
                ("event_type", "string"),
                ("page", "string"),
                ("product_id", "integer"),
                ("category", "string"),
                ("device_type", "string"),
                ("ip_address", "string"),
                ("user_agent", "string")
            ]
            
            data = []
            df = spark.createDataFrame(data, schema=[f"{name} {dtype}" for name, dtype in schema])
        
        # Create batch request
        batch_request = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="default_runtime_data_connector",
            data_asset_name="user_activity_bronze",
            batch_identifiers={
                "batch_id": "default_batch",
                "layer": "bronze",
                "dataset": "user_activity"
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
                "user_id", "session_id", "timestamp", "event_type", "page",
                "product_id", "category", "device_type", "ip_address", "user_agent"
            ]
        )
        
        # Column level expectations - user_id
        validator.expect_column_to_exist("user_id")
        validator.expect_column_values_to_not_be_null("user_id")
        validator.expect_column_values_to_match_regex("user_id", r"^user_\d+$")
        
        # Column level expectations - session_id
        validator.expect_column_to_exist("session_id")
        validator.expect_column_values_to_not_be_null("session_id")
        validator.expect_column_values_to_match_regex("session_id", r"^session_\w+$")
        
        # Column level expectations - timestamp
        validator.expect_column_to_exist("timestamp")
        validator.expect_column_values_to_not_be_null("timestamp")
        validator.expect_column_values_to_match_strftime_format("timestamp", "%Y-%m-%d %H:%M:%S")
        
        # Column level expectations - event_type
        validator.expect_column_to_exist("event_type")
        validator.expect_column_values_to_not_be_null("event_type")
        validator.expect_column_values_to_be_in_set(
            "event_type", 
            [
                "page_view", "add_to_cart", "remove_from_cart", 
                "checkout", "purchase", "search", "login", "logout"
            ]
        )
        
        # Column level expectations - page
        validator.expect_column_to_exist("page")
        validator.expect_column_values_to_not_be_null("page", mostly=0.9)  # Allow some nulls
        
        # Column level expectations - product_id
        validator.expect_column_to_exist("product_id")
        # product_id can be null for non-product pages
        validator.expect_column_values_to_be_of_type("product_id", "IntegerType", mostly=0.8)
        
        # Column level expectations - category
        validator.expect_column_to_exist("category")
        # category can be null for non-product pages
        
        # Column level expectations - device_type
        validator.expect_column_to_exist("device_type")
        validator.expect_column_values_to_not_be_null("device_type", mostly=0.95)  # Allow some nulls
        validator.expect_column_values_to_be_in_set(
            "device_type", 
            ["desktop", "mobile", "tablet", "unknown"],
            mostly=0.95  # Allow some unexpected values
        )
        
        # Column level expectations - ip_address
        validator.expect_column_to_exist("ip_address")
        validator.expect_column_values_to_not_be_null("ip_address", mostly=0.95)  # Allow some nulls
        validator.expect_column_values_to_match_regex(
            "ip_address", 
            r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$",
            mostly=0.95  # Allow some invalid formats
        )
        
        # Column level expectations - user_agent
        validator.expect_column_to_exist("user_agent")
        validator.expect_column_values_to_not_be_null("user_agent", mostly=0.95)  # Allow some nulls
        
        # Save expectation suite
        validator.save_expectation_suite(discard_failed_expectations=False)
        
        logger.info(f"Expectation suite '{suite_name}' created successfully")
        return suite
    
    except Exception as e:
        logger.error(f"Error creating expectations for user activity data: {str(e)}")
        raise

def create_orders_expectations(context, suite_name="orders_bronze_suite"):
    """
    Create expectations for orders data in the bronze layer.
    
    Args:
        context: Great Expectations data context
        suite_name: Name of the expectation suite
    
    Returns:
        ExpectationSuite: Created expectation suite
    """
    logger.info(f"Creating expectations for orders data: {suite_name}")
    
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
            df = spark.read.parquet(os.path.join(BRONZE_DIR, "orders"))
        except Exception:
            # Create empty DataFrame with expected schema if data doesn't exist
            schema = [
                ("order_id", "string"),
                ("customer_id", "string"),
                ("order_date", "string"),
                ("order_status", "string"),
                ("items_str", "string"),
                ("payment_method", "string"),
                ("shipping_method", "string"),
                ("shipping_cost", "double"),
                ("tax", "double"),
                ("discount", "double"),
                ("coupon_code", "string"),
                ("subtotal", "double"),
                ("total", "double"),
                ("billing_address_str", "string"),
                ("shipping_address_str", "string")
            ]
            
            data = []
            df = spark.createDataFrame(data, schema=[f"{name} {dtype}" for name, dtype in schema])
        
        # Create batch request
        batch_request = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="default_runtime_data_connector",
            data_asset_name="orders_bronze",
            batch_identifiers={
                "batch_id": "default_batch",
                "layer": "bronze",
                "dataset": "orders"
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
                "order_id", "customer_id", "order_date", "order_status", "items_str",
                "payment_method", "shipping_method", "shipping_cost", "tax", "discount",
                "coupon_code", "subtotal", "total", "billing_address_str", "shipping_address_str"
            ]
        )
        
        # Column level expectations - order_id
        validator.expect_column_to_exist("order_id")
        validator.expect_column_values_to_not_be_null("order_id")
        validator.expect_column_values_to_match_regex("order_id", r"^ORD\d+$")
        validator.expect_column_values_to_be_unique("order_id")
        
        # Column level expectations - customer_id
        validator.expect_column_to_exist("customer_id")
        validator.expect_column_values_to_not_be_null("customer_id")
        validator.expect_column_values_to_match_regex("customer_id", r"^user_\d+$")
        
        # Column level expectations - order_date
        validator.expect_column_to_exist("order_date")
        validator.expect_column_values_to_not_be_null("order_date")
        validator.expect_column_values_to_match_strftime_format("order_date", "%Y-%m-%d")
        
        # Column level expectations - order_status
        validator.expect_column_to_exist("order_status")
        validator.expect_column_values_to_not_be_null("order_status")
        validator.expect_column_values_to_be_in_set(
            "order_status", 
            ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]
        )
        
        # Column level expectations - items_str
        validator.expect_column_to_exist("items_str")
        validator.expect_column_values_to_not_be_null("items_str")
        validator.expect_column_values_to_match_json_schema(
            "items_str",
            {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "product_id": {"type": "integer"},
                        "category": {"type": "string"},
                        "price": {"type": "number"},
                        "quantity": {"type": "integer"}
                    },
                    "required": ["product_id", "price", "quantity"]
                }
            },
            mostly=0.95  # Allow some invalid formats
        )
        
        # Column level expectations - payment_method
        validator.expect_column_to_exist("payment_method")
        validator.expect_column_values_to_not_be_null("payment_method")
        validator.expect_column_values_to_be_in_set(
            "payment_method", 
            ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "gift_card", "bank_transfer"]
        )
        
        # Column level expectations - shipping_method
        validator.expect_column_to_exist("shipping_method")
        validator.expect_column_values_to_not_be_null("shipping_method")
        validator.expect_column_values_to_be_in_set(
            "shipping_method", 
            ["standard", "express", "next_day", "pickup", "free"]
        )
        
        # Column level expectations - shipping_cost
        validator.expect_column_to_exist("shipping_cost")
        validator.expect_column_values_to_not_be_null("shipping_cost")
        validator.expect_column_values_to_be_of_type("shipping_cost", "DoubleType")
        validator.expect_column_values_to_be_between("shipping_cost", min_value=0, max_value=100)
        
        # Column level expectations - tax
        validator.expect_column_to_exist("tax")
        validator.expect_column_values_to_not_be_null("tax")
        validator.expect_column_values_to_be_of_type("tax", "DoubleType")
        validator.expect_column_values_to_be_between("tax", min_value=0, max_value=None)
        
        # Column level expectations - discount
        validator.expect_column_to_exist("discount")
        validator.expect_column_values_to_not_be_null("discount", mostly=0.95)  # Allow some nulls
        validator.expect_column_values_to_be_of_type("discount", "DoubleType")
        validator.expect_column_values_to_be_between("discount", min_value=0, max_value=None)
        
        # Column level expectations - coupon_code
        validator.expect_column_to_exist("coupon_code")
        # coupon_code can be null or empty
        
        # Column level expectations - subtotal
        validator.expect_column_to_exist("subtotal")
        validator.expect_column_values_to_not_be_null("subtotal")
        validator.expect_column_values_to_be_of_type("subtotal", "DoubleType")
        validator.expect_column_values_to_be_between("subtotal", min_value=0, max_value=None)
        
        # Column level expectations - total
        validator.expect_column_to_exist("total")
        validator.expect_column_values_to_not_be_null("total")
        validator.expect_column_values_to_be_of_type("total", "DoubleType")
        validator.expect_column_values_to_be_between("total", min_value=0, max_value=None)
        
        # Column level expectations - billing_address_str and shipping_address_str
        for address_field in ["billing_address_str", "shipping_address_str"]:
            validator.expect_column_to_exist(address_field)
            validator.expect_column_values_to_not_be_null(address_field)
  
(Content truncated due to size limit. Use line ranges to read in chunks)