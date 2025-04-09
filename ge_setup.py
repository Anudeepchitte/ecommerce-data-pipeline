#!/usr/bin/env python3
"""
Great Expectations Configuration Module for E-commerce Data Pipeline

This module sets up Great Expectations for data validation across the data pipeline.
It configures data sources, expectation suites, and validation contexts.

Features:
- Great Expectations context setup
- Data source configuration for bronze, silver, and gold layers
- Expectation suite initialization
- Validation result storage configuration
"""

import os
import sys
import logging
import great_expectations as ge
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/validation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("data_validation")

# Create logs directory if it doesn't exist
os.makedirs("../logs", exist_ok=True)

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
GE_DIR = os.path.join(BASE_DIR, "data_validation", "great_expectations")

# Create Great Expectations directories
os.makedirs(os.path.join(GE_DIR, "expectations"), exist_ok=True)
os.makedirs(os.path.join(GE_DIR, "validations"), exist_ok=True)
os.makedirs(os.path.join(GE_DIR, "checkpoints"), exist_ok=True)
os.makedirs(os.path.join(GE_DIR, "plugins"), exist_ok=True)

def create_data_context_config():
    """
    Create a Great Expectations data context configuration.
    
    Returns:
        DataContextConfig: Great Expectations data context configuration
    """
    return DataContextConfig(
        config_version=3.0,
        plugins_directory=os.path.join(GE_DIR, "plugins"),
        stores={
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(GE_DIR, "expectations"),
                }
            },
            "validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(GE_DIR, "validations"),
                }
            },
            "evaluation_parameter_store": {
                "class_name": "EvaluationParameterStore"
            },
            "checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(GE_DIR, "checkpoints"),
                }
            },
        },
        expectations_store_name="expectations_store",
        validations_store_name="validations_store",
        evaluation_parameter_store_name="evaluation_parameter_store",
        checkpoint_store_name="checkpoint_store",
        data_docs_sites={
            "local_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(GE_DIR, "data_docs", "local_site"),
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                },
            }
        },
        anonymous_usage_statistics={
            "enabled": False
        }
    )

def create_data_context():
    """
    Create a Great Expectations data context.
    
    Returns:
        BaseDataContext: Great Expectations data context
    """
    logger.info("Creating Great Expectations data context")
    
    try:
        # Create data context config
        context_config = create_data_context_config()
        
        # Create data context
        context = BaseDataContext(project_config=context_config)
        
        # Create data docs directory
        os.makedirs(os.path.join(GE_DIR, "data_docs", "local_site"), exist_ok=True)
        
        logger.info("Great Expectations data context created successfully")
        return context
    
    except Exception as e:
        logger.error(f"Error creating Great Expectations data context: {str(e)}")
        raise

def configure_spark_datasource(context):
    """
    Configure Spark datasource for Great Expectations.
    
    Args:
        context: Great Expectations data context
    
    Returns:
        str: Name of the configured datasource
    """
    logger.info("Configuring Spark datasource")
    
    try:
        # Configure Spark datasource
        datasource_name = "spark_datasource"
        
        # Add Spark datasource
        context.add_datasource(
            name=datasource_name,
            class_name="Datasource",
            execution_engine={
                "class_name": "SparkDFExecutionEngine",
                "spark_config": {
                    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                    "spark.sql.warehouse.dir": os.path.join(DATA_DIR, "warehouse"),
                    "spark.executor.memory": "2g",
                    "spark.driver.memory": "2g",
                }
            },
            data_connectors={
                "default_runtime_data_connector": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["batch_id", "layer", "dataset"],
                },
                "default_inferred_data_connector": {
                    "class_name": "InferredAssetFilesystemDataConnector",
                    "base_directory": DATA_DIR,
                    "default_regex": {
                        "pattern": r"(bronze|silver|gold)/(.*)/",
                        "group_names": ["layer", "dataset"],
                    },
                },
            },
        )
        
        logger.info(f"Spark datasource '{datasource_name}' configured successfully")
        return datasource_name
    
    except Exception as e:
        logger.error(f"Error configuring Spark datasource: {str(e)}")
        raise

def create_expectation_suite(context, suite_name, overwrite_existing=False):
    """
    Create a Great Expectations expectation suite.
    
    Args:
        context: Great Expectations data context
        suite_name: Name of the expectation suite
        overwrite_existing: Whether to overwrite existing suite
    
    Returns:
        ExpectationSuite: Created expectation suite
    """
    logger.info(f"Creating expectation suite '{suite_name}'")
    
    try:
        # Create expectation suite
        suite = context.create_expectation_suite(
            expectation_suite_name=suite_name,
            overwrite_existing=overwrite_existing
        )
        
        logger.info(f"Expectation suite '{suite_name}' created successfully")
        return suite
    
    except Exception as e:
        logger.error(f"Error creating expectation suite '{suite_name}': {str(e)}")
        raise

def create_checkpoint(context, checkpoint_name, expectation_suite_name, datasource_name, batch_request=None):
    """
    Create a Great Expectations checkpoint.
    
    Args:
        context: Great Expectations data context
        checkpoint_name: Name of the checkpoint
        expectation_suite_name: Name of the expectation suite
        datasource_name: Name of the datasource
        batch_request: Batch request configuration
    
    Returns:
        Checkpoint: Created checkpoint
    """
    logger.info(f"Creating checkpoint '{checkpoint_name}'")
    
    try:
        # Create default batch request if not provided
        if batch_request is None:
            batch_request = {
                "datasource_name": datasource_name,
                "data_connector_name": "default_runtime_data_connector",
                "data_asset_name": checkpoint_name,
                "batch_identifiers": {
                    "batch_id": "default_batch",
                    "layer": "default_layer",
                    "dataset": "default_dataset"
                },
            }
        
        # Create checkpoint configuration
        checkpoint_config = {
            "name": checkpoint_name,
            "config_version": 1.0,
            "template_name": None,
            "validations": [
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": expectation_suite_name
                }
            ]
        }
        
        # Create checkpoint
        context.add_checkpoint(**checkpoint_config)
        
        logger.info(f"Checkpoint '{checkpoint_name}' created successfully")
        return checkpoint_config
    
    except Exception as e:
        logger.error(f"Error creating checkpoint '{checkpoint_name}': {str(e)}")
        raise

def setup_great_expectations():
    """
    Set up Great Expectations for the e-commerce data pipeline.
    
    Returns:
        tuple: (context, datasource_name)
    """
    logger.info("Setting up Great Expectations")
    
    try:
        # Create data context
        context = create_data_context()
        
        # Configure Spark datasource
        datasource_name = configure_spark_datasource(context)
        
        # Create basic expectation suites for each layer
        bronze_suite = create_expectation_suite(context, "bronze_expectations", overwrite_existing=True)
        silver_suite = create_expectation_suite(context, "silver_expectations", overwrite_existing=True)
        gold_suite = create_expectation_suite(context, "gold_expectations", overwrite_existing=True)
        
        # Create basic checkpoints
        bronze_checkpoint = create_checkpoint(context, "bronze_checkpoint", "bronze_expectations", datasource_name)
        silver_checkpoint = create_checkpoint(context, "silver_checkpoint", "silver_expectations", datasource_name)
        gold_checkpoint = create_checkpoint(context, "gold_checkpoint", "gold_expectations", datasource_name)
        
        logger.info("Great Expectations setup completed successfully")
        return context, datasource_name
    
    except Exception as e:
        logger.error(f"Error setting up Great Expectations: {str(e)}")
        raise

def main():
    """Main function to set up Great Expectations."""
    try:
        # Set up Great Expectations
        context, datasource_name = setup_great_expectations()
        
        # Build data docs
        context.build_data_docs()
        
        logger.info(f"Great Expectations data docs built at {os.path.join(GE_DIR, 'data_docs', 'local_site')}")
        logger.info("Great Expectations setup completed successfully")
    
    except Exception as e:
        logger.error(f"Error in Great Expectations setup: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
