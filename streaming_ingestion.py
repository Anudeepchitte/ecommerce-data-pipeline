#!/usr/bin/env python3
"""
Streaming Data Ingestion Module for E-commerce Data Pipeline

This module handles the streaming ingestion of user activity data in JSON format.
It simulates a streaming data source and processes data in micro-batches.

Features:
- Real-time data ingestion
- Schema validation
- Bad data handling
- Writing to bronze layer
"""

import os
import json
import time
import random
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampType

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/streaming_ingestion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("streaming_ingestion")

# Create logs directory if it doesn't exist
os.makedirs("../logs", exist_ok=True)

# Create data directories if they don't exist
os.makedirs("../data/bronze/user_activity_stream", exist_ok=True)

def get_spark_session():
    """Initialize and return a Spark session."""
    return (SparkSession.builder
            .appName("E-commerce Streaming Data Pipeline")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", "../data/warehouse")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "2g")
            .master("local[*]")
            .getOrCreate())

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

def simulate_streaming_source(input_file, output_dir, batch_size=10, interval_seconds=5):
    """
    Simulate a streaming source by reading from a JSON file and writing batches to output directory.
    
    Args:
        input_file: Path to input JSON file
        output_dir: Directory to write streaming batches
        batch_size: Number of records per batch
        interval_seconds: Seconds between batches
    """
    logger.info(f"Starting streaming simulation from {input_file}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Read all data from JSON file
        with open(input_file, 'r') as f:
            all_data = json.load(f)
        
        # Process data in batches
        total_records = len(all_data)
        batch_count = 0
        
        for i in range(0, total_records, batch_size):
            batch_count += 1
            batch = all_data[i:i+batch_size]
            
            # Add some randomness to simulate real-time data
            for record in batch:
                # Randomly modify some records to simulate schema evolution
                if random.random() < 0.05:  # 5% chance
                    record["new_field"] = f"value_{random.randint(1, 100)}"
                
                # Randomly corrupt some records to simulate bad data
                if random.random() < 0.02:  # 2% chance
                    if random.random() < 0.5:
                        # Remove a required field
                        if "user_id" in record:
                            del record["user_id"]
                    else:
                        # Add an invalid value
                        record["event_type"] = None
            
            # Write batch to output directory
            batch_file = os.path.join(output_dir, f"batch_{batch_count}.json")
            with open(batch_file, 'w') as f:
                json.dump(batch, f)
            
            logger.info(f"Written batch {batch_count} with {len(batch)} records to {batch_file}")
            
            # Sleep to simulate time between batches
            time.sleep(interval_seconds)
            
            # Stop after 10 batches for demo purposes
            if batch_count >= 10:
                break
        
        logger.info(f"Streaming simulation completed with {batch_count} batches")
    
    except Exception as e:
        logger.error(f"Error in streaming simulation: {str(e)}")
        raise

def process_streaming_data(spark, schema, input_dir, checkpoint_dir, output_dir):
    """
    Process streaming data using Spark Structured Streaming.
    
    Args:
        spark: SparkSession
        schema: Schema for the streaming data
        input_dir: Directory to read streaming data from
        checkpoint_dir: Directory for streaming checkpoints
        output_dir: Directory to write processed data
    """
    logger.info(f"Starting streaming processing from {input_dir}")
    
    # Create checkpoint directory if it doesn't exist
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    try:
        # Create streaming DataFrame
        streaming_df = (
            spark.readStream
            .schema(schema)
            .json(input_dir)
        )
        
        # Validate data - filter out records with missing required fields
        validated_df = streaming_df.filter(
            streaming_df.user_id.isNotNull() & 
            streaming_df.session_id.isNotNull() & 
            streaming_df.timestamp.isNotNull() & 
            streaming_df.event_type.isNotNull()
        )
        
        # Write to bronze layer
        query = (
            validated_df.writeStream
            .format("parquet")
            .option("checkpointLocation", checkpoint_dir)
            .option("path", output_dir)
            .partitionBy("event_type")
            .trigger(processingTime="10 seconds")
            .start()
        )
        
        # Wait for termination
        query.awaitTermination(timeout=300)  # 5 minutes timeout for demo
        
        logger.info("Streaming processing completed")
    
    except Exception as e:
        logger.error(f"Error in streaming processing: {str(e)}")
        raise

def main():
    """Main function to run streaming data ingestion."""
    # Initialize Spark session
    spark = get_spark_session()
    
    try:
        # Define paths
        input_file = "../data/user_activity.json"
        streaming_dir = "../data/streaming_source"
        checkpoint_dir = "../data/checkpoints/user_activity_stream"
        output_dir = "../data/bronze/user_activity_stream"
        
        # Get schema
        schema = get_user_activity_schema()
        
        # Start streaming simulation in a separate process
        # In a real environment, this would be a separate service
        # For demo purposes, we'll simulate it here
        simulate_streaming_source(input_file, streaming_dir)
        
        # Process streaming data
        process_streaming_data(spark, schema, streaming_dir, checkpoint_dir, output_dir)
        
        logger.info("Streaming data ingestion completed successfully")
    
    except Exception as e:
        logger.error(f"Error in streaming data ingestion: {str(e)}")
    
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()
