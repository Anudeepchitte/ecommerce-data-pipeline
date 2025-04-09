#!/usr/bin/env python3
"""
Data Ingestion Module for E-commerce Data Pipeline

This module handles the ingestion of data from three sources:
- User activity data (JSON format, streaming)
- Orders data (CSV format, daily batch)
- Inventory data (XLSX format, weekly batch)

It includes functionality for:
- Schema validation
- Schema evolution handling
- Bad data detection and handling
- Data normalization
"""

import os
import json
import pandas as pd
import datetime
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, ArrayType, MapType, TimestampType

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/ingestion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("data_ingestion")

# Create logs directory if it doesn't exist
os.makedirs("../logs", exist_ok=True)

# Create data directories if they don't exist
os.makedirs("../data/bronze", exist_ok=True)
os.makedirs("../data/silver", exist_ok=True)
os.makedirs("../data/gold", exist_ok=True)

# Initialize Spark session
def get_spark_session():
    """Initialize and return a Spark session."""
    return (SparkSession.builder
            .appName("E-commerce Data Pipeline")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", "../data/warehouse")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "2g")
            .master("local[*]")
            .getOrCreate())

# Schema definitions
def get_user_activity_schema():
    """Define and return the schema for user activity data."""
    return StructType([
        StructField("user_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("event_type", StringType(), False),
        StructField("device_type", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("user_agent", StringType(), True),
        # Optional fields based on event type
        StructField("page", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("cart_total", DoubleType(), True),
        StructField("items_count", IntegerType(), True),
        StructField("order_id", StringType(), True),
        StructField("items", ArrayType(
            StructType([
                StructField("product_id", IntegerType(), True),
                StructField("category", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("quantity", IntegerType(), True)
            ])
        ), True)
    ])

def get_orders_schema():
    """Define and return the schema for orders data."""
    return StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", TimestampType(), False),
        StructField("order_status", StringType(), False),
        StructField("payment_method", StringType(), True),
        StructField("shipping_method", StringType(), True),
        StructField("subtotal", DoubleType(), False),
        StructField("tax", DoubleType(), True),
        StructField("shipping_cost", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("coupon_code", StringType(), True),
        StructField("total", DoubleType(), False),
        # Complex fields will be handled separately
        StructField("items_str", StringType(), True),
        StructField("billing_address_str", StringType(), True),
        StructField("shipping_address_str", StringType(), True)
    ])

def get_inventory_schema():
    """Define and return the schema for inventory data."""
    return StructType([
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("brand", StringType(), True),
        StructField("supplier", StringType(), True),
        StructField("cost_price", DoubleType(), True),
        StructField("retail_price", DoubleType(), False),
        StructField("current_stock", IntegerType(), False),
        StructField("reorder_level", IntegerType(), True),
        StructField("reorder_quantity", IntegerType(), True),
        StructField("warehouse_location", StringType(), True),
        StructField("last_restock_date", TimestampType(), True),
        StructField("is_active", BooleanType(), True)
    ])

# Data ingestion functions
def ingest_user_activity(spark, input_path, output_path):
    """
    Ingest user activity data from JSON file.
    
    Args:
        spark: SparkSession
        input_path: Path to input JSON file
        output_path: Path to output Parquet directory
    
    Returns:
        DataFrame with ingested data
    """
    logger.info(f"Ingesting user activity data from {input_path}")
    
    try:
        # Read JSON file
        df = spark.read.json(input_path)
        
        # Get current schema
        current_schema = df.schema
        
        # Get expected schema
        expected_schema = get_user_activity_schema()
        
        # Handle schema evolution
        df = handle_schema_evolution(spark, df, current_schema, expected_schema)
        
        # Validate data
        df = validate_user_activity_data(df)
        
        # Write to Parquet
        df.write.mode("overwrite").partitionBy("event_type").parquet(output_path)
        
        logger.info(f"Successfully ingested {df.count()} user activity records")
        return df
    
    except Exception as e:
        logger.error(f"Error ingesting user activity data: {str(e)}")
        raise

def ingest_orders(spark, input_path, output_path):
    """
    Ingest orders data from CSV file.
    
    Args:
        spark: SparkSession
        input_path: Path to input CSV file
        output_path: Path to output Parquet directory
    
    Returns:
        DataFrame with ingested data
    """
    logger.info(f"Ingesting orders data from {input_path}")
    
    try:
        # Read CSV file
        df = spark.read.option("header", "true").csv(input_path)
        
        # Get current schema
        current_schema = df.schema
        
        # Get expected schema
        expected_schema = get_orders_schema()
        
        # Handle schema evolution
        df = handle_schema_evolution(spark, df, current_schema, expected_schema)
        
        # Parse complex fields
        df = parse_orders_complex_fields(df)
        
        # Validate data
        df = validate_orders_data(df)
        
        # Write to Parquet
        df.write.mode("overwrite").partitionBy("order_status").parquet(output_path)
        
        logger.info(f"Successfully ingested {df.count()} order records")
        return df
    
    except Exception as e:
        logger.error(f"Error ingesting orders data: {str(e)}")
        raise

def ingest_inventory(spark, input_path, output_path):
    """
    Ingest inventory data from XLSX file.
    
    Args:
        spark: SparkSession
        input_path: Path to input XLSX file
        output_path: Path to output Parquet directory
    
    Returns:
        DataFrame with ingested data
    """
    logger.info(f"Ingesting inventory data from {input_path}")
    
    try:
        # Read XLSX file using pandas (PySpark doesn't directly support XLSX)
        pdf = pd.read_excel(input_path)
        
        # Convert pandas DataFrame to Spark DataFrame
        df = spark.createDataFrame(pdf)
        
        # Get current schema
        current_schema = df.schema
        
        # Get expected schema
        expected_schema = get_inventory_schema()
        
        # Handle schema evolution
        df = handle_schema_evolution(spark, df, current_schema, expected_schema)
        
        # Validate data
        df = validate_inventory_data(df)
        
        # Write to Parquet
        df.write.mode("overwrite").partitionBy("category").parquet(output_path)
        
        logger.info(f"Successfully ingested {df.count()} inventory records")
        return df
    
    except Exception as e:
        logger.error(f"Error ingesting inventory data: {str(e)}")
        raise

# Schema evolution and validation functions
def handle_schema_evolution(spark, df, current_schema, expected_schema):
    """
    Handle schema evolution by adding missing columns and casting to expected types.
    
    Args:
        spark: SparkSession
        df: DataFrame to evolve
        current_schema: Current schema of the DataFrame
        expected_schema: Expected schema
    
    Returns:
        DataFrame with evolved schema
    """
    # Get current and expected field names
    current_fields = {field.name: field.dataType for field in current_schema.fields}
    expected_fields = {field.name: field.dataType for field in expected_schema.fields}
    
    # Add missing columns
    for field_name, data_type in expected_fields.items():
        if field_name not in current_fields:
            logger.info(f"Adding missing column: {field_name}")
            df = df.withColumn(field_name, spark.createDataFrame([(None,)], [field_name]).select(field_name))
    
    # Cast columns to expected types
    for field_name, data_type in expected_fields.items():
        if field_name in current_fields and current_fields[field_name] != data_type:
            logger.info(f"Casting column {field_name} from {current_fields[field_name]} to {data_type}")
            try:
                df = df.withColumn(field_name, df[field_name].cast(data_type))
            except Exception as e:
                logger.warning(f"Could not cast column {field_name}: {str(e)}")
    
    return df

def validate_user_activity_data(df):
    """
    Validate user activity data and handle bad records.
    
    Args:
        df: DataFrame to validate
    
    Returns:
        Validated DataFrame
    """
    # Filter out records with missing required fields
    validated_df = df.filter(
        df.user_id.isNotNull() & 
        df.session_id.isNotNull() & 
        df.timestamp.isNotNull() & 
        df.event_type.isNotNull()
    )
    
    # Log number of bad records
    bad_records_count = df.count() - validated_df.count()
    if bad_records_count > 0:
        logger.warning(f"Filtered out {bad_records_count} bad user activity records")
    
    return validated_df

def parse_orders_complex_fields(df):
    """
    Parse complex fields in orders data.
    
    Args:
        df: DataFrame with orders data
    
    Returns:
        DataFrame with parsed complex fields
    """
    # This would be more complex in a real implementation
    # For now, we'll just rename the string fields
    return df.withColumnRenamed("items", "items_str") \
             .withColumnRenamed("billing_address", "billing_address_str") \
             .withColumnRenamed("shipping_address", "shipping_address_str")

def validate_orders_data(df):
    """
    Validate orders data and handle bad records.
    
    Args:
        df: DataFrame to validate
    
    Returns:
        Validated DataFrame
    """
    # Filter out records with missing required fields
    validated_df = df.filter(
        df.order_id.isNotNull() & 
        df.customer_id.isNotNull() & 
        df.order_date.isNotNull() & 
        df.order_status.isNotNull() &
        df.subtotal.isNotNull() &
        df.total.isNotNull()
    )
    
    # Log number of bad records
    bad_records_count = df.count() - validated_df.count()
    if bad_records_count > 0:
        logger.warning(f"Filtered out {bad_records_count} bad order records")
    
    return validated_df

def validate_inventory_data(df):
    """
    Validate inventory data and handle bad records.
    
    Args:
        df: DataFrame to validate
    
    Returns:
        Validated DataFrame
    """
    # Filter out records with missing required fields
    validated_df = df.filter(
        df.product_id.isNotNull() & 
        df.product_name.isNotNull() & 
        df.category.isNotNull() & 
        df.retail_price.isNotNull() &
        df.current_stock.isNotNull()
    )
    
    # Log number of bad records
    bad_records_count = df.count() - validated_df.count()
    if bad_records_count > 0:
        logger.warning(f"Filtered out {bad_records_count} bad inventory records")
    
    return validated_df

def main():
    """Main function to run data ingestion."""
    # Initialize Spark session
    spark = get_spark_session()
    
    try:
        # Ingest user activity data
        user_activity_df = ingest_user_activity(
            spark, 
            "../data/user_activity.json", 
            "../data/bronze/user_activity"
        )
        
        # Ingest orders data
        orders_df = ingest_orders(
            spark, 
            "../data/orders.csv", 
            "../data/bronze/orders"
        )
        
        # Ingest inventory data
        inventory_df = ingest_inventory(
            spark, 
            "../data/inventory.xlsx", 
            "../data/bronze/inventory"
        )
        
        logger.info("Data ingestion completed successfully")
    
    except Exception as e:
        logger.error(f"Error in data ingestion: {str(e)}")
    
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()
