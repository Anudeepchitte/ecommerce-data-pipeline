#!/usr/bin/env python3
"""
Schema Registry Module for E-commerce Data Pipeline

This module provides a centralized schema registry for the data pipeline.
It defines and manages schemas for all data sources and handles schema evolution.

Features:
- Centralized schema definitions
- Schema versioning
- Schema compatibility checking
- Schema evolution support
"""

import json
import os
import logging
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, ArrayType, TimestampType

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/schema_registry.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("schema_registry")

# Create logs directory if it doesn't exist
os.makedirs("../logs", exist_ok=True)

# Create schema registry directory if it doesn't exist
os.makedirs("../schema_registry", exist_ok=True)

class SchemaRegistry:
    """Schema Registry for managing data schemas."""
    
    def __init__(self, registry_dir="../schema_registry"):
        """
        Initialize the schema registry.
        
        Args:
            registry_dir: Directory to store schema definitions
        """
        self.registry_dir = registry_dir
        os.makedirs(registry_dir, exist_ok=True)
        logger.info(f"Initialized schema registry at {registry_dir}")
    
    def register_schema(self, source_name, schema, version=None):
        """
        Register a schema for a data source.
        
        Args:
            source_name: Name of the data source
            schema: PySpark StructType schema
            version: Schema version (defaults to timestamp if not provided)
        
        Returns:
            Schema version
        """
        if version is None:
            version = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Create source directory if it doesn't exist
        source_dir = os.path.join(self.registry_dir, source_name)
        os.makedirs(source_dir, exist_ok=True)
        
        # Convert schema to JSON string
        schema_json = self._schema_to_json(schema)
        
        # Write schema to file
        schema_file = os.path.join(source_dir, f"v{version}.json")
        with open(schema_file, 'w') as f:
            f.write(schema_json)
        
        logger.info(f"Registered schema for {source_name} version {version}")
        return version
    
    def get_schema(self, source_name, version="latest"):
        """
        Get a schema for a data source.
        
        Args:
            source_name: Name of the data source
            version: Schema version (or "latest" for the latest version)
        
        Returns:
            PySpark StructType schema
        """
        source_dir = os.path.join(self.registry_dir, source_name)
        
        if not os.path.exists(source_dir):
            raise ValueError(f"No schemas found for source {source_name}")
        
        if version == "latest":
            # Get the latest version
            versions = [f for f in os.listdir(source_dir) if f.startswith("v") and f.endswith(".json")]
            if not versions:
                raise ValueError(f"No schemas found for source {source_name}")
            
            versions.sort(reverse=True)
            version = versions[0].replace("v", "").replace(".json", "")
        
        schema_file = os.path.join(source_dir, f"v{version}.json")
        
        if not os.path.exists(schema_file):
            raise ValueError(f"Schema version {version} not found for source {source_name}")
        
        # Read schema from file
        with open(schema_file, 'r') as f:
            schema_json = f.read()
        
        # Convert JSON to schema
        schema = self._json_to_schema(schema_json)
        
        logger.info(f"Retrieved schema for {source_name} version {version}")
        return schema
    
    def check_compatibility(self, source_name, new_schema, version="latest"):
        """
        Check if a new schema is compatible with an existing schema.
        
        Args:
            source_name: Name of the data source
            new_schema: New PySpark StructType schema
            version: Existing schema version to check against
        
        Returns:
            Tuple of (is_compatible, compatibility_issues)
        """
        try:
            existing_schema = self.get_schema(source_name, version)
        except ValueError:
            # If no existing schema, then it's compatible
            return True, []
        
        # Check compatibility
        is_compatible, issues = self._check_schema_compatibility(existing_schema, new_schema)
        
        if is_compatible:
            logger.info(f"New schema is compatible with {source_name} version {version}")
        else:
            logger.warning(f"New schema is not compatible with {source_name} version {version}: {issues}")
        
        return is_compatible, issues
    
    def evolve_schema(self, source_name, new_schema, version=None):
        """
        Evolve a schema for a data source.
        
        Args:
            source_name: Name of the data source
            new_schema: New PySpark StructType schema
            version: New schema version (defaults to timestamp if not provided)
        
        Returns:
            New schema version
        """
        # Check compatibility with latest schema
        is_compatible, issues = self.check_compatibility(source_name, new_schema)
        
        if not is_compatible:
            logger.warning(f"Schema evolution may cause issues: {issues}")
        
        # Register new schema
        new_version = self.register_schema(source_name, new_schema, version)
        
        logger.info(f"Evolved schema for {source_name} to version {new_version}")
        return new_version
    
    def _schema_to_json(self, schema):
        """
        Convert a PySpark StructType schema to JSON string.
        
        Args:
            schema: PySpark StructType schema
        
        Returns:
            JSON string representation of the schema
        """
        schema_dict = {
            "type": "struct",
            "fields": []
        }
        
        for field in schema.fields:
            field_dict = {
                "name": field.name,
                "type": self._datatype_to_dict(field.dataType),
                "nullable": field.nullable
            }
            schema_dict["fields"].append(field_dict)
        
        return json.dumps(schema_dict, indent=2)
    
    def _datatype_to_dict(self, datatype):
        """
        Convert a PySpark DataType to a dictionary.
        
        Args:
            datatype: PySpark DataType
        
        Returns:
            Dictionary representation of the DataType
        """
        if isinstance(datatype, StructType):
            return {
                "type": "struct",
                "fields": [
                    {
                        "name": field.name,
                        "type": self._datatype_to_dict(field.dataType),
                        "nullable": field.nullable
                    }
                    for field in datatype.fields
                ]
            }
        elif isinstance(datatype, ArrayType):
            return {
                "type": "array",
                "elementType": self._datatype_to_dict(datatype.elementType),
                "containsNull": datatype.containsNull
            }
        else:
            return str(datatype)
    
    def _json_to_schema(self, schema_json):
        """
        Convert a JSON string to a PySpark StructType schema.
        
        Args:
            schema_json: JSON string representation of the schema
        
        Returns:
            PySpark StructType schema
        """
        schema_dict = json.loads(schema_json)
        return self._dict_to_schema(schema_dict)
    
    def _dict_to_schema(self, schema_dict):
        """
        Convert a dictionary to a PySpark StructType schema.
        
        Args:
            schema_dict: Dictionary representation of the schema
        
        Returns:
            PySpark StructType schema
        """
        if schema_dict["type"] == "struct":
            fields = []
            for field_dict in schema_dict["fields"]:
                field_type = self._dict_to_datatype(field_dict["type"])
                field = StructField(
                    field_dict["name"],
                    field_type,
                    field_dict["nullable"]
                )
                fields.append(field)
            return StructType(fields)
        else:
            raise ValueError(f"Expected struct type, got {schema_dict['type']}")
    
    def _dict_to_datatype(self, type_dict):
        """
        Convert a dictionary to a PySpark DataType.
        
        Args:
            type_dict: Dictionary representation of the DataType
        
        Returns:
            PySpark DataType
        """
        if isinstance(type_dict, str):
            # Simple type
            if type_dict == "StringType()":
                return StringType()
            elif type_dict == "IntegerType()":
                return IntegerType()
            elif type_dict == "DoubleType()":
                return DoubleType()
            elif type_dict == "BooleanType()":
                return BooleanType()
            elif type_dict == "TimestampType()":
                return TimestampType()
            else:
                raise ValueError(f"Unsupported data type: {type_dict}")
        elif isinstance(type_dict, dict):
            # Complex type
            if type_dict["type"] == "struct":
                return self._dict_to_schema(type_dict)
            elif type_dict["type"] == "array":
                element_type = self._dict_to_datatype(type_dict["elementType"])
                return ArrayType(element_type, type_dict["containsNull"])
            else:
                raise ValueError(f"Unsupported complex type: {type_dict['type']}")
        else:
            raise ValueError(f"Unexpected type format: {type(type_dict)}")
    
    def _check_schema_compatibility(self, old_schema, new_schema):
        """
        Check if a new schema is compatible with an old schema.
        
        Args:
            old_schema: Old PySpark StructType schema
            new_schema: New PySpark StructType schema
        
        Returns:
            Tuple of (is_compatible, compatibility_issues)
        """
        issues = []
        
        # Get field maps
        old_fields = {field.name: field for field in old_schema.fields}
        new_fields = {field.name: field for field in new_schema.fields}
        
        # Check for removed fields
        for field_name in old_fields:
            if field_name not in new_fields:
                issues.append(f"Field '{field_name}' was removed")
        
        # Check for type changes
        for field_name, old_field in old_fields.items():
            if field_name in new_fields:
                new_field = new_fields[field_name]
                
                # Check if type changed
                if str(old_field.dataType) != str(new_field.dataType):
                    issues.append(f"Field '{field_name}' changed type from {old_field.dataType} to {new_field.dataType}")
                
                # Check if nullability changed from nullable=false to nullable=true
                if old_field.nullable and not new_field.nullable:
                    issues.append(f"Field '{field_name}' changed from nullable to non-nullable")
        
        # If there are any issues, it's not compatible
        is_compatible = len(issues) == 0
        
        return is_compatible, issues

# Define schemas
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

def initialize_registry():
    """Initialize the schema registry with default schemas."""
    registry = SchemaRegistry()
    
    # Register schemas
    registry.register_schema("user_activity", get_user_activity_schema(), "1.0.0")
    registry.register_schema("orders", get_orders_schema(), "1.0.0")
    registry.register_schema("inventory", get_inventory_schema(), "1.0.0")
    
    logger.info("Schema registry initialized with default schemas")
    return registry

if __name__ == "__main__":
    # Initialize registry
    registry = initialize_registry()
    
    # Example of schema evolution
    # Evolve user activity schema to add a new field
    new_user_activity_schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("event
(Content truncated due to size limit. Use line ranges to read in chunks)