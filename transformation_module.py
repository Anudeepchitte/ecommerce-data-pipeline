#!/usr/bin/env python3
"""
Data Processing and Transformation Module for E-commerce Data Pipeline

This module handles the processing and transformation of data from the bronze layer
to the silver and gold layers. It includes data cleaning, normalization, joining,
and KPI calculations.

Features:
- Data cleaning and normalization
- Handling nulls, duplicates, and type mismatches
- Joining datasets
- Computing KPIs (total revenue, top products, user retention)
"""

import os
import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, when, lit, sum as spark_sum, count, avg, max as spark_max
from pyspark.sql.functions import datediff, to_date, date_format, month, year, dayofmonth
from pyspark.sql.functions import explode, from_json, expr, rank, dense_rank, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("data_processing")

# Create logs directory if it doesn't exist
os.makedirs("../logs", exist_ok=True)

# Create data directories if they don't exist
os.makedirs("../data/silver", exist_ok=True)
os.makedirs("../data/gold", exist_ok=True)

def get_spark_session():
    """Initialize and return a Spark session."""
    return (SparkSession.builder
            .appName("E-commerce Data Processing")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", "../data/warehouse")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "2g")
            .master("local[*]")
            .getOrCreate())

# Schema for parsing complex fields
def get_items_schema():
    """Define and return the schema for order items."""
    return ArrayType(
        StructType([
            StructField("product_id", IntegerType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", IntegerType(), True)
        ])
    )

def get_address_schema():
    """Define and return the schema for addresses."""
    return StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("country", StringType(), True)
    ])

# Data cleaning and normalization functions
def clean_user_activity(spark, input_path, output_path):
    """
    Clean and normalize user activity data.
    
    Args:
        spark: SparkSession
        input_path: Path to input Parquet directory
        output_path: Path to output Parquet directory
    
    Returns:
        DataFrame with cleaned data
    """
    logger.info(f"Cleaning user activity data from {input_path}")
    
    try:
        # Read Parquet files
        df = spark.read.parquet(input_path)
        
        # Handle nulls
        df = df.na.fill({
            "device_type": "unknown",
            "ip_address": "0.0.0.0",
            "user_agent": "unknown"
        })
        
        # Convert timestamp to date format
        df = df.withColumn("event_date", to_date(col("timestamp")))
        
        # Normalize event types (lowercase)
        df = df.withColumn("event_type", expr("lower(event_type)"))
        
        # Remove duplicates
        df = df.dropDuplicates(["user_id", "session_id", "timestamp", "event_type"])
        
        # Write to Parquet
        df.write.mode("overwrite").partitionBy("event_date", "event_type").parquet(output_path)
        
        logger.info(f"Successfully cleaned {df.count()} user activity records")
        return df
    
    except Exception as e:
        logger.error(f"Error cleaning user activity data: {str(e)}")
        raise

def clean_orders(spark, input_path, output_path):
    """
    Clean and normalize orders data.
    
    Args:
        spark: SparkSession
        input_path: Path to input Parquet directory
        output_path: Path to output Parquet directory
    
    Returns:
        DataFrame with cleaned data
    """
    logger.info(f"Cleaning orders data from {input_path}")
    
    try:
        # Read Parquet files
        df = spark.read.parquet(input_path)
        
        # Parse complex fields
        items_schema = get_items_schema()
        address_schema = get_address_schema()
        
        # Clean and parse items
        df = df.withColumn(
            "items", 
            when(col("items_str").isNotNull(), 
                 from_json(col("items_str").cast("string"), items_schema)
            ).otherwise(lit(None))
        )
        
        # Clean and parse addresses
        df = df.withColumn(
            "billing_address", 
            when(col("billing_address_str").isNotNull(), 
                 from_json(col("billing_address_str").cast("string"), address_schema)
            ).otherwise(lit(None))
        )
        
        df = df.withColumn(
            "shipping_address", 
            when(col("shipping_address_str").isNotNull(), 
                 from_json(col("shipping_address_str").cast("string"), address_schema)
            ).otherwise(lit(None))
        )
        
        # Drop original string columns
        df = df.drop("items_str", "billing_address_str", "shipping_address_str")
        
        # Handle nulls
        df = df.na.fill({
            "tax": 0.0,
            "shipping_cost": 0.0,
            "discount": 0.0,
            "coupon_code": ""
        })
        
        # Convert order_date to date format
        df = df.withColumn("order_date", to_date(col("order_date")))
        
        # Add year, month, day columns for partitioning
        df = df.withColumn("order_year", year(col("order_date")))
        df = df.withColumn("order_month", month(col("order_date")))
        df = df.withColumn("order_day", dayofmonth(col("order_date")))
        
        # Normalize order status (lowercase)
        df = df.withColumn("order_status", expr("lower(order_status)"))
        
        # Remove duplicates
        df = df.dropDuplicates(["order_id"])
        
        # Write to Parquet
        df.write.mode("overwrite").partitionBy("order_year", "order_month", "order_status").parquet(output_path)
        
        logger.info(f"Successfully cleaned {df.count()} order records")
        return df
    
    except Exception as e:
        logger.error(f"Error cleaning orders data: {str(e)}")
        raise

def clean_inventory(spark, input_path, output_path):
    """
    Clean and normalize inventory data.
    
    Args:
        spark: SparkSession
        input_path: Path to input Parquet directory
        output_path: Path to output Parquet directory
    
    Returns:
        DataFrame with cleaned data
    """
    logger.info(f"Cleaning inventory data from {input_path}")
    
    try:
        # Read Parquet files
        df = spark.read.parquet(input_path)
        
        # Handle nulls
        df = df.na.fill({
            "brand": "unknown",
            "supplier": "unknown",
            "cost_price": 0.0,
            "reorder_level": 10,
            "reorder_quantity": 50,
            "warehouse_location": "unknown",
            "is_active": True
        })
        
        # Convert last_restock_date to date format
        df = df.withColumn("last_restock_date", to_date(col("last_restock_date")))
        
        # Add days_since_restock column
        current_date = spark.sql("SELECT current_date()").collect()[0][0]
        df = df.withColumn(
            "days_since_restock", 
            when(col("last_restock_date").isNotNull(),
                 datediff(lit(current_date), col("last_restock_date"))
            ).otherwise(lit(999))
        )
        
        # Add stock_status column
        df = df.withColumn(
            "stock_status",
            when(col("current_stock") <= 0, "out_of_stock")
            .when(col("current_stock") < col("reorder_level"), "low_stock")
            .otherwise("in_stock")
        )
        
        # Normalize category (lowercase)
        df = df.withColumn("category", expr("lower(category)"))
        
        # Remove duplicates
        df = df.dropDuplicates(["product_id"])
        
        # Write to Parquet
        df.write.mode("overwrite").partitionBy("category", "stock_status").parquet(output_path)
        
        logger.info(f"Successfully cleaned {df.count()} inventory records")
        return df
    
    except Exception as e:
        logger.error(f"Error cleaning inventory data: {str(e)}")
        raise

# Data joining functions
def create_product_dimension(spark, inventory_df, output_path):
    """
    Create product dimension table.
    
    Args:
        spark: SparkSession
        inventory_df: Inventory DataFrame
        output_path: Path to output Parquet directory
    
    Returns:
        DataFrame with product dimension
    """
    logger.info("Creating product dimension table")
    
    try:
        # Select relevant columns
        product_df = inventory_df.select(
            "product_id",
            "product_name",
            "category",
            "brand",
            "supplier",
            "retail_price",
            "cost_price",
            "is_active"
        )
        
        # Write to Parquet
        product_df.write.mode("overwrite").partitionBy("category").parquet(output_path)
        
        logger.info(f"Successfully created product dimension with {product_df.count()} records")
        return product_df
    
    except Exception as e:
        logger.error(f"Error creating product dimension: {str(e)}")
        raise

def create_customer_dimension(spark, orders_df, user_activity_df, output_path):
    """
    Create customer dimension table.
    
    Args:
        spark: SparkSession
        orders_df: Orders DataFrame
        user_activity_df: User Activity DataFrame
        output_path: Path to output Parquet directory
    
    Returns:
        DataFrame with customer dimension
    """
    logger.info("Creating customer dimension table")
    
    try:
        # Get customer IDs from orders
        customer_df = orders_df.select("customer_id").distinct()
        
        # Join with user activity to get additional customer information
        user_df = user_activity_df.select("user_id", "device_type").distinct()
        
        # In a real scenario, we would have more customer data
        # For this demo, we'll create a simple dimension with limited data
        customer_df = customer_df.join(
            user_df,
            customer_df.customer_id == user_df.user_id,
            "left"
        ).select(
            customer_df.customer_id,
            user_df.device_type
        )
        
        # Fill missing values
        customer_df = customer_df.na.fill({"device_type": "unknown"})
        
        # Write to Parquet
        customer_df.write.mode("overwrite").parquet(output_path)
        
        logger.info(f"Successfully created customer dimension with {customer_df.count()} records")
        return customer_df
    
    except Exception as e:
        logger.error(f"Error creating customer dimension: {str(e)}")
        raise

def create_date_dimension(spark, start_date, end_date, output_path):
    """
    Create date dimension table.
    
    Args:
        spark: SparkSession
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        output_path: Path to output Parquet directory
    
    Returns:
        DataFrame with date dimension
    """
    logger.info(f"Creating date dimension table from {start_date} to {end_date}")
    
    try:
        # Create date range
        date_df = spark.sql(f"""
            SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as date
        """)
        
        # Add date attributes
        date_df = date_df.withColumn("year", year(col("date")))
        date_df = date_df.withColumn("month", month(col("date")))
        date_df = date_df.withColumn("day", dayofmonth(col("date")))
        date_df = date_df.withColumn("month_name", date_format(col("date"), "MMMM"))
        date_df = date_df.withColumn("day_of_week", date_format(col("date"), "EEEE"))
        date_df = date_df.withColumn("quarter", expr("quarter(date)"))
        
        # Write to Parquet
        date_df.write.mode("overwrite").partitionBy("year", "month").parquet(output_path)
        
        logger.info(f"Successfully created date dimension with {date_df.count()} records")
        return date_df
    
    except Exception as e:
        logger.error(f"Error creating date dimension: {str(e)}")
        raise

def create_sales_fact(spark, orders_df, output_path):
    """
    Create sales fact table.
    
    Args:
        spark: SparkSession
        orders_df: Orders DataFrame
        output_path: Path to output Parquet directory
    
    Returns:
        DataFrame with sales fact
    """
    logger.info("Creating sales fact table")
    
    try:
        # Explode items array to get one row per item
        sales_df = orders_df.select(
            "order_id",
            "customer_id",
            "order_date",
            "order_status",
            "payment_method",
            "shipping_method",
            "subtotal",
            "tax",
            "shipping_cost",
            "discount",
            "total",
            "order_year",
            "order_month",
            "order_day",
            explode("items").alias("item")
        )
        
        # Extract item fields
        sales_df = sales_df.select(
            "order_id",
            "customer_id",
            "order_date",
            "order_status",
            "payment_method",
            "shipping_method",
            "subtotal",
            "tax",
            "shipping_cost",
            "discount",
            "total",
            "order_year",
            "order_month",
            "order_day",
            col("item.product_id").alias("product_id"),
            col("item.category").alias("product_category"),
            col("item.price").alias("product_price"),
            col("item.quantity").alias("quantity")
        )
        
        # Calculate item total
        sales_df = sales_df.withColumn(
            "item_total",
            col("product_price") * col("quantity")
        )
        
        # Write to Parquet
        sales_df.write.mode("overwrite").partitionBy("order_year", "order_month").parquet(output_path)
        
        logger.info(f"Successfully created sales fact with {sales_df.count()} records")
        return sales_df
    
    except Exception as e:
        logger.error(f"Error creating sales fact: {str(e)}")
        raise

def create_user_activity_fact(spark, user_activity_df, output_path):
    """
    Create user activity fact table.
    
    Args:
        spark: SparkSession
        user_activity_df: User Activity DataFrame
        output_path: Path to output Parquet directory
    
    Returns:
        DataFrame with user activity fact
    """
    logger.info("Creating user activity fact table")
    
    try:
        # Select relevant columns
        activity_df = user_activity_df.select(
            "user_id",
            "session_id",
            "timestamp",
            "event_date",
            "event_type",
            "device_type",
            "product_id",
            "category",
            "page"
        )
        
        # Add year, month, day columns for partitioning
        activity_df = activity_df.withColumn("activity_year", year(col("event_date")))
        activity_df = activity_df.withColumn("activity_mon
(Content truncated due to size limit. Use line ranges to read in chunks)