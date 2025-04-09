#!/usr/bin/env python3
"""
Test Runner for E-commerce Data Pipeline Validation

This script tests the complete data validation extension by:
1. Generating sample test data for all layers
2. Running validation suites against the test data
3. Testing the reporting system
4. Testing the alerting system
5. Testing the optimization strategies
6. Generating a comprehensive test report
"""

import os
import sys
import logging
import json
import datetime
import pandas as pd
import numpy as np
import great_expectations as ge
from pyspark.sql import SparkSession
import random
import time

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import validation modules
from data_validation.ge_setup import create_data_context, configure_spark_datasource
from data_validation.bronze_validation import create_bronze_expectations
from data_validation.silver_validation import create_silver_expectations
from data_validation.gold_validation import create_gold_expectations
from data_validation.validation_reporting import ValidationReporter
from data_validation.monitoring_alerts import process_validation_results, integrate_with_monitoring_system
from data_validation.validation_optimization import ValidationOptimizer, optimize_validation_pipeline

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/validation_test.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("validation_test")

# Create logs directory if it doesn't exist
os.makedirs("../logs", exist_ok=True)

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEST_DATA_DIR = os.path.join(BASE_DIR, "data_validation", "test_data")
TEST_RESULTS_DIR = os.path.join(BASE_DIR, "data_validation", "test_results")

# Create directories if they don't exist
os.makedirs(TEST_DATA_DIR, exist_ok=True)
os.makedirs(TEST_RESULTS_DIR, exist_ok=True)

class ValidationTestRunner:
    """Class for testing the data validation extension."""
    
    def __init__(self):
        """Initialize the test runner."""
        self.spark = self._create_spark_session()
        self.context = create_data_context()
        self.datasource_name = configure_spark_datasource(self.context)
        self.test_results = {
            "timestamp": datetime.datetime.now().isoformat(),
            "overall_status": "pending",
            "components": {},
            "performance": {},
            "summary": {}
        }
    
    def _create_spark_session(self):
        """
        Create a Spark session for testing.
        
        Returns:
            SparkSession: Spark session
        """
        return (SparkSession.builder
                .appName("E-commerce Data Validation Test")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.executor.memory", "2g")
                .config("spark.driver.memory", "2g")
                .master("local[*]")
                .getOrCreate())
    
    def generate_test_data(self):
        """
        Generate test data for all layers.
        
        Returns:
            dict: Dictionary of test data DataFrames
        """
        logger.info("Generating test data for all layers")
        
        test_data = {}
        
        # Generate bronze layer test data
        test_data["bronze"] = self._generate_bronze_test_data()
        
        # Generate silver layer test data
        test_data["silver"] = self._generate_silver_test_data()
        
        # Generate gold layer test data
        test_data["gold"] = self._generate_gold_test_data()
        
        # Save test data
        self._save_test_data(test_data)
        
        return test_data
    
    def _generate_bronze_test_data(self):
        """
        Generate bronze layer test data.
        
        Returns:
            dict: Dictionary of bronze layer test DataFrames
        """
        logger.info("Generating bronze layer test data")
        
        bronze_data = {}
        
        # Generate user activity data (JSON format simulation)
        user_activity_data = []
        for i in range(1000):
            timestamp = datetime.datetime.now() - datetime.timedelta(minutes=random.randint(1, 1440))
            user_activity_data.append({
                "user_id": f"user_{random.randint(1, 100)}",
                "session_id": f"session_{random.randint(1, 500)}",
                "timestamp": timestamp.isoformat(),
                "event_type": random.choice(["page_view", "click", "add_to_cart", "purchase", "login", "logout"]),
                "page_url": f"/page_{random.randint(1, 50)}",
                "ip_address": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
                "user_agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(80, 90)}.0.{random.randint(1000, 5000)}.{random.randint(100, 999)}",
                "referrer": random.choice([None, "https://google.com", "https://facebook.com", "https://twitter.com", "https://instagram.com"]),
                "device_type": random.choice(["desktop", "mobile", "tablet"]),
                "browser": random.choice(["chrome", "firefox", "safari", "edge"]),
                "os": random.choice(["windows", "macos", "linux", "ios", "android"]),
                "country": random.choice(["US", "UK", "CA", "AU", "DE", "FR", "JP", "IN", "BR", "MX"]),
                "city": random.choice(["New York", "London", "Toronto", "Sydney", "Berlin", "Paris", "Tokyo", "Mumbai", "Sao Paulo", "Mexico City"]),
                "language": random.choice(["en-US", "en-GB", "fr-FR", "de-DE", "ja-JP", "es-ES"]),
                "custom_properties": {
                    "property1": random.choice([None, "value1", "value2", "value3"]),
                    "property2": random.choice([None, 1, 2, 3, 4, 5]),
                    "property3": random.choice([None, True, False])
                }
            })
        
        # Introduce some data quality issues
        for i in range(50):
            idx = random.randint(0, len(user_activity_data) - 1)
            field = random.choice(["user_id", "session_id", "timestamp", "event_type", "page_url", "ip_address"])
            if field == "timestamp":
                user_activity_data[idx][field] = "invalid_timestamp"
            elif field == "ip_address":
                user_activity_data[idx][field] = "invalid_ip"
            else:
                user_activity_data[idx][field] = None
        
        user_activity_df = self.spark.createDataFrame(user_activity_data)
        bronze_data["user_activity"] = user_activity_df
        
        # Generate orders data (CSV format simulation)
        orders_data = []
        for i in range(500):
            order_date = datetime.datetime.now() - datetime.timedelta(days=random.randint(1, 30))
            orders_data.append({
                "order_id": f"order_{i+1}",
                "customer_id": f"user_{random.randint(1, 100)}",
                "order_date": order_date.strftime("%Y-%m-%d"),
                "order_status": random.choice(["pending", "processing", "shipped", "delivered", "cancelled"]),
                "payment_method": random.choice(["credit_card", "paypal", "bank_transfer", "cash_on_delivery"]),
                "shipping_method": random.choice(["standard", "express", "next_day", "pickup"]),
                "order_total": round(random.uniform(10, 500), 2),
                "tax_amount": round(random.uniform(1, 50), 2),
                "shipping_amount": round(random.uniform(5, 25), 2),
                "discount_amount": round(random.uniform(0, 50), 2),
                "coupon_code": random.choice([None, "SUMMER10", "WELCOME20", "FREESHIP"]),
                "items_count": random.randint(1, 10),
                "shipping_address": f"{random.randint(1, 999)} Main St, {random.choice(['New York', 'London', 'Toronto', 'Sydney', 'Berlin', 'Paris', 'Tokyo', 'Mumbai', 'Sao Paulo', 'Mexico City'])}",
                "billing_address": f"{random.randint(1, 999)} Main St, {random.choice(['New York', 'London', 'Toronto', 'Sydney', 'Berlin', 'Paris', 'Tokyo', 'Mumbai', 'Sao Paulo', 'Mexico City'])}",
                "notes": random.choice([None, "Please leave at door", "Gift wrap", "Call before delivery"])
            })
        
        # Introduce some data quality issues
        for i in range(25):
            idx = random.randint(0, len(orders_data) - 1)
            field = random.choice(["order_id", "customer_id", "order_date", "order_status", "order_total", "tax_amount"])
            if field in ["order_total", "tax_amount", "shipping_amount"]:
                orders_data[idx][field] = random.choice([None, -1 * orders_data[idx][field], "invalid"])
            elif field == "order_date":
                orders_data[idx][field] = "invalid_date"
            else:
                orders_data[idx][field] = None
        
        orders_df = self.spark.createDataFrame(orders_data)
        bronze_data["orders"] = orders_df
        
        # Generate inventory data (XLSX format simulation)
        inventory_data = []
        for i in range(200):
            last_updated = datetime.datetime.now() - datetime.timedelta(hours=random.randint(1, 168))
            inventory_data.append({
                "product_id": f"product_{i+1}",
                "product_name": f"Product {i+1}",
                "category": random.choice(["Electronics", "Clothing", "Home", "Books", "Toys", "Sports", "Beauty", "Food", "Health", "Automotive"]),
                "subcategory": random.choice(["Phones", "Laptops", "Shirts", "Pants", "Furniture", "Fiction", "Action Figures", "Outdoor", "Skincare", "Snacks", "Vitamins", "Accessories"]),
                "brand": f"Brand {random.randint(1, 20)}",
                "supplier": f"Supplier {random.randint(1, 10)}",
                "current_stock": random.randint(0, 1000),
                "reorder_level": random.randint(10, 100),
                "reorder_quantity": random.randint(50, 200),
                "cost_price": round(random.uniform(5, 250), 2),
                "selling_price": round(random.uniform(10, 500), 2),
                "discount_percentage": random.randint(0, 50),
                "tax_rate": random.choice([0, 5, 10, 15, 20]),
                "warehouse_location": f"Warehouse {random.choice(['A', 'B', 'C', 'D'])}, Aisle {random.randint(1, 20)}, Shelf {random.randint(1, 50)}",
                "last_updated": last_updated.isoformat(),
                "is_active": random.choice([True, False]),
                "notes": random.choice([None, "Seasonal item", "Limited stock", "New arrival", "Clearance"])
            })
        
        # Introduce some data quality issues
        for i in range(20):
            idx = random.randint(0, len(inventory_data) - 1)
            field = random.choice(["product_id", "product_name", "category", "current_stock", "cost_price", "selling_price"])
            if field in ["current_stock", "cost_price", "selling_price"]:
                inventory_data[idx][field] = random.choice([None, -1 * inventory_data[idx][field], "invalid"])
            else:
                inventory_data[idx][field] = None
        
        inventory_df = self.spark.createDataFrame(inventory_data)
        bronze_data["inventory"] = inventory_df
        
        return bronze_data
    
    def _generate_silver_test_data(self):
        """
        Generate silver layer test data.
        
        Returns:
            dict: Dictionary of silver layer test DataFrames
        """
        logger.info("Generating silver layer test data")
        
        silver_data = {}
        
        # Generate cleaned user activity data
        user_activity_data = []
        for i in range(1000):
            timestamp = datetime.datetime.now() - datetime.timedelta(minutes=random.randint(1, 1440))
            user_activity_data.append({
                "user_id": f"user_{random.randint(1, 100)}",
                "session_id": f"session_{random.randint(1, 500)}",
                "timestamp": timestamp.isoformat(),
                "event_type": random.choice(["page_view", "click", "add_to_cart", "purchase", "login", "logout"]),
                "page_url": f"/page_{random.randint(1, 50)}",
                "ip_address": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
                "device_type": random.choice(["desktop", "mobile", "tablet"]),
                "browser": random.choice(["chrome", "firefox", "safari", "edge"]),
                "os": random.choice(["windows", "macos", "linux", "ios", "android"]),
                "country": random.choice(["US", "UK", "CA", "AU", "DE", "FR", "JP", "IN", "BR", "MX"]),
                "city": random.choice(["New York", "London", "Toronto", "Sydney", "Berlin", "Paris", "Tokyo", "Mumbai", "Sao Paulo", "Mexico City"]),
                "language": random.choice(["en-US", "en-GB", "fr-FR", "de-DE", "ja-JP", "es-ES"]),
                "referrer_domain": random.choice([None, "google.com", "facebook.com", "twitter.com", "instagram.com"]),
                "session_duration_seconds": random.randint(10, 3600),
                "is_new_user": random.choice([True, False]),
                "is_logged_in": random.choice([True, False])
            })
        
        # Introduce some data quality issues (fewer than bronze)
        for i in range(20):
            idx = random.randint(0, len(user_activity_data) - 1)
            field = random.choice(["user_id", "session_id", "timestamp", "event_type", "page_url"])
            if field == "timestamp":
                user_activity_data[idx][field] = "invalid_timestamp"
            else:
                user_activity_data[idx][field] = None
        
        user_activity_df = self.spark.createDataFrame(user_activity_data)
        silver_data["user_activity"] = user_activity_df
        
        # Generate cleaned orders data
        orders_data = []
        for i in range(500):
            order_date = datetime.datetime.now() - datetime.timedelta(days=random.randint(1, 30))
            subtotal = round(random.uniform(10, 450), 2)
            tax_amount = round(subtotal * 0.1, 2)
            shipping_amount = round(random.uniform(5, 25), 2)
            discount_amount = round(random.uniform(0, 50), 2)
            order_total = subtotal + tax_amount + shipping_amount - discount_amount
            
            orders_data.append({
                "order_id": f"order_{i+1}",
                "customer_id": f"user_{random.randint(1, 100)}",
                "order_date": order_date.strftime("%Y-%m-%d"),
                "order_status": random.choice(["pending", "processing", "shipped", "delivered", "cancelled"]),
                "payment_method": random.choice(["credit_card", "paypal", "bank_transfer", "cash_on_delivery"]),
                "shipping_method": random.choice(["standard", "express", "next_day", "pickup"]),
                "subtotal": subtotal,
                "tax_amount": tax_amount,
                "shipping_amount": shipping_amount,
                "discount_amount": discount_amount,
                "order_total": order_total,
                "coupon_code": random.choice([None, "SUMMER10", "WELCOME20", "FREESHIP"]),
                "items_count": random.randint(1, 10),
                "shipping_country": random.choice(["US", "UK", "CA", "AU", "DE", "FR", "JP", "IN", "BR", "MX"]),
                "shipping_city": random.choice(["New York", "London", "Toronto", "Sydney", "Berlin", "Paris", "Tokyo", "Mumbai", "Sao Paulo", "Mexico City"]),
                "is_first_order": random.choice([True, False]),
                "days_to_ship": random.randint(1, 7),
                "days_to_deliver": random
(Content truncated due to size limit. Use line ranges to read in chunks)