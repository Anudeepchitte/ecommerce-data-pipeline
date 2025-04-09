#!/usr/bin/env python3
"""
Main Entry Point for E-commerce Data Pipeline Validation

This script serves as the main entry point for the data validation extension.
It provides a command-line interface to run validation on different layers
and datasets, generate reports, and view validation results.
"""

import os
import sys
import logging
import argparse
import datetime
import json
from pyspark.sql import SparkSession

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import validation modules
from data_validation.ge_setup import create_data_context, configure_spark_datasource
from data_validation.bronze_validation import create_bronze_expectations, validate_bronze_data
from data_validation.silver_validation import create_silver_expectations, validate_silver_data
from data_validation.gold_validation import create_gold_expectations, validate_gold_data
from data_validation.validation_reporting import ValidationReporter
from data_validation.monitoring_alerts import process_validation_results, integrate_with_monitoring_system
from data_validation.validation_optimization import ValidationOptimizer, optimize_validation_pipeline
from data_validation.test_runner import ValidationTestRunner

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/validation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("validation_main")

# Create logs directory if it doesn't exist
os.makedirs("../logs", exist_ok=True)

def create_spark_session():
    """
    Create a Spark session.
    
    Returns:
        SparkSession: Spark session
    """
    return (SparkSession.builder
            .appName("E-commerce Data Validation")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "2g")
            .master("local[*]")
            .getOrCreate())

def setup_validation_environment():
    """
    Set up the validation environment.
    
    Returns:
        tuple: (context, datasource_name, spark)
    """
    logger.info("Setting up validation environment")
    
    # Create Spark session
    spark = create_spark_session()
    
    # Create data context
    context = create_data_context()
    
    # Configure Spark datasource
    datasource_name = configure_spark_datasource(context)
    
    return context, datasource_name, spark

def run_validation(args):
    """
    Run validation on specified layer and dataset.
    
    Args:
        args: Command-line arguments
    
    Returns:
        dict: Validation results
    """
    logger.info(f"Running validation on {args.layer} layer, dataset: {args.dataset}")
    
    # Set up environment
    context, datasource_name, spark = setup_validation_environment()
    
    # Load data
    try:
        df = spark.read.parquet(args.data_path)
        logger.info(f"Loaded data from {args.data_path}: {df.count()} rows")
    except Exception as e:
        logger.error(f"Error loading data from {args.data_path}: {str(e)}")
        return {"status": "error", "message": f"Error loading data: {str(e)}"}
    
    # Run validation based on layer
    try:
        if args.layer == "bronze":
            # Create expectations if needed
            if args.create_expectations:
                create_bronze_expectations(context)
            
            # Run validation
            result = validate_bronze_data(context, df, args.dataset)
        elif args.layer == "silver":
            # Create expectations if needed
            if args.create_expectations:
                create_silver_expectations(context)
            
            # Run validation
            result = validate_silver_data(context, df, args.dataset)
        elif args.layer == "gold":
            # Create expectations if needed
            if args.create_expectations:
                create_gold_expectations(context)
            
            # Run validation
            result = validate_gold_data(context, df, args.dataset)
        else:
            logger.error(f"Invalid layer: {args.layer}")
            return {"status": "error", "message": f"Invalid layer: {args.layer}"}
        
        logger.info(f"Validation completed: success={result.success}")
        
        # Generate report if requested
        if args.report:
            reporter = ValidationReporter(context)
            reporter.add_validation_result(result, args.layer, args.dataset)
            report_path = reporter.generate_summary_report()
            logger.info(f"Generated report at {report_path}")
        
        # Process alerts if requested
        if args.alerts:
            reporter = ValidationReporter(context)
            reporter.add_validation_result(result, args.layer, args.dataset)
            alert_results = process_validation_results(reporter.summary)
            logger.info(f"Processed alerts: {alert_results}")
        
        return {
            "status": "success",
            "success": result.success,
            "statistics": result.statistics,
            "result": result
        }
    
    except Exception as e:
        logger.error(f"Error running validation: {str(e)}")
        return {"status": "error", "message": f"Error running validation: {str(e)}"}

def run_optimized_validation(args):
    """
    Run optimized validation on specified layer and dataset.
    
    Args:
        args: Command-line arguments
    
    Returns:
        dict: Validation results
    """
    logger.info(f"Running optimized validation on {args.layer} layer, dataset: {args.dataset}")
    
    # Set up environment
    context, datasource_name, spark = setup_validation_environment()
    
    # Load data
    try:
        df = spark.read.parquet(args.data_path)
        logger.info(f"Loaded data from {args.data_path}: {df.count()} rows")
    except Exception as e:
        logger.error(f"Error loading data from {args.data_path}: {str(e)}")
        return {"status": "error", "message": f"Error loading data: {str(e)}"}
    
    # Get expectation suite name
    if args.layer == "bronze":
        suite_name = f"{args.dataset}_bronze_suite"
    elif args.layer == "silver":
        suite_name = f"{args.dataset}_silver_suite"
    elif args.layer == "gold":
        suite_name = f"{args.dataset}_gold_suite"
    else:
        logger.error(f"Invalid layer: {args.layer}")
        return {"status": "error", "message": f"Invalid layer: {args.layer}"}
    
    # Create expectations if needed
    if args.create_expectations:
        if args.layer == "bronze":
            create_bronze_expectations(context)
        elif args.layer == "silver":
            create_silver_expectations(context)
        elif args.layer == "gold":
            create_gold_expectations(context)
    
    # Run optimized validation
    try:
        optimizer = ValidationOptimizer()
        result, stats = optimizer.optimize_validation(
            context, df, args.layer, args.dataset, suite_name
        )
        
        logger.info(f"Optimized validation completed: success={result.success}")
        logger.info(f"Optimization stats: {stats}")
        
        # Generate report if requested
        if args.report:
            reporter = ValidationReporter(context)
            reporter.add_validation_result(result, args.layer, args.dataset)
            report_path = reporter.generate_summary_report()
            logger.info(f"Generated report at {report_path}")
        
        # Process alerts if requested
        if args.alerts:
            reporter = ValidationReporter(context)
            reporter.add_validation_result(result, args.layer, args.dataset)
            alert_results = process_validation_results(reporter.summary)
            logger.info(f"Processed alerts: {alert_results}")
        
        return {
            "status": "success",
            "success": result.success,
            "statistics": result.statistics,
            "optimization_stats": stats,
            "result": result
        }
    
    except Exception as e:
        logger.error(f"Error running optimized validation: {str(e)}")
        return {"status": "error", "message": f"Error running optimized validation: {str(e)}"}

def run_tests(args):
    """
    Run tests for the validation extension.
    
    Args:
        args: Command-line arguments
    
    Returns:
        dict: Test results
    """
    logger.info("Running tests for validation extension")
    
    try:
        # Create and run test runner
        test_runner = ValidationTestRunner()
        test_results = test_runner.run_tests()
        
        # Print summary
        print("\n=== Test Summary ===")
        print(f"Overall Status: {test_results['overall_status']}")
        print(f"Execution Time: {test_results['summary']['execution_time']:.2f} seconds")
        print(f"Components Tested: {test_results['summary']['components_tested']}")
        print(f"Components Passed: {test_results['summary']['components_passed']}")
        print(f"Components Failed: {test_results['summary']['components_failed']}")
        print(f"Test Report: {test_results['report_path']}")
        
        return test_results
    
    except Exception as e:
        logger.error(f"Error running tests: {str(e)}")
        return {"status": "error", "message": f"Error running tests: {str(e)}"}

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="E-commerce Data Pipeline Validation")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Validate command
    validate_parser = subparsers.add_parser("validate", help="Run validation on a dataset")
    validate_parser.add_argument("--layer", required=True, choices=["bronze", "silver", "gold"], help="Data layer")
    validate_parser.add_argument("--dataset", required=True, help="Dataset name")
    validate_parser.add_argument("--data-path", required=True, help="Path to data")
    validate_parser.add_argument("--create-expectations", action="store_true", help="Create expectations if they don't exist")
    validate_parser.add_argument("--report", action="store_true", help="Generate validation report")
    validate_parser.add_argument("--alerts", action="store_true", help="Process alerts for validation results")
    
    # Optimize command
    optimize_parser = subparsers.add_parser("optimize", help="Run optimized validation on a dataset")
    optimize_parser.add_argument("--layer", required=True, choices=["bronze", "silver", "gold"], help="Data layer")
    optimize_parser.add_argument("--dataset", required=True, help="Dataset name")
    optimize_parser.add_argument("--data-path", required=True, help="Path to data")
    optimize_parser.add_argument("--create-expectations", action="store_true", help="Create expectations if they don't exist")
    optimize_parser.add_argument("--report", action="store_true", help="Generate validation report")
    optimize_parser.add_argument("--alerts", action="store_true", help="Process alerts for validation results")
    
    # Test command
    test_parser = subparsers.add_parser("test", help="Run tests for validation extension")
    
    args = parser.parse_args()
    
    if args.command == "validate":
        result = run_validation(args)
    elif args.command == "optimize":
        result = run_optimized_validation(args)
    elif args.command == "test":
        result = run_tests(args)
    else:
        parser.print_help()
        sys.exit(1)
    
    if result.get("status") == "error":
        logger.error(result["message"])
        sys.exit(1)
    
    sys.exit(0 if result.get("success", True) else 1)

if __name__ == "__main__":
    main()
