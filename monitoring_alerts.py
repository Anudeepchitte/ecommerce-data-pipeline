#!/usr/bin/env python3
"""
Monitoring Alerts Module for E-commerce Data Pipeline

This module integrates data validation results with the monitoring system to provide
alerts when data quality thresholds are breached. It includes configurable alert
thresholds, notification channels, and escalation workflows.

Features:
- Alert integration with validation results
- Configurable quality thresholds
- Multiple notification channels (email, Slack, etc.)
- Alert severity levels and escalation
- Alert history tracking
"""

import os
import sys
import logging
import json
import datetime
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
import great_expectations as ge

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import validation modules
from data_validation.ge_setup import create_data_context
from data_validation.validation_reporting import ValidationReporter

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/monitoring_alerts.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("monitoring_alerts")

# Create logs directory if it doesn't exist
os.makedirs("../logs", exist_ok=True)

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(BASE_DIR, "data_validation", "config")
ALERTS_DIR = os.path.join(BASE_DIR, "data_validation", "alerts")
HISTORY_DIR = os.path.join(ALERTS_DIR, "history")

# Create directories if they don't exist
os.makedirs(CONFIG_DIR, exist_ok=True)
os.makedirs(ALERTS_DIR, exist_ok=True)
os.makedirs(HISTORY_DIR, exist_ok=True)

# Default configuration
DEFAULT_CONFIG = {
    "alert_thresholds": {
        "global": {
            "success_rate": 90.0,  # Minimum success rate (percentage)
            "failed_validations": 3,  # Maximum allowed failed validations
            "failed_expectations": 5  # Maximum allowed failed expectations
        },
        "bronze": {
            "success_rate": 85.0,
            "failed_validations": 5,
            "failed_expectations": 10
        },
        "silver": {
            "success_rate": 90.0,
            "failed_validations": 3,
            "failed_expectations": 7
        },
        "gold": {
            "success_rate": 95.0,
            "failed_validations": 1,
            "failed_expectations": 3
        },
        "datasets": {
            "user_activity": {
                "success_rate": 90.0,
                "failed_validations": 2,
                "failed_expectations": 5
            },
            "orders": {
                "success_rate": 95.0,
                "failed_validations": 1,
                "failed_expectations": 3
            },
            "inventory": {
                "success_rate": 95.0,
                "failed_validations": 1,
                "failed_expectations": 3
            },
            "dim_customer": {
                "success_rate": 98.0,
                "failed_validations": 0,
                "failed_expectations": 1
            },
            "dim_product": {
                "success_rate": 98.0,
                "failed_validations": 0,
                "failed_expectations": 1
            },
            "fact_sales": {
                "success_rate": 98.0,
                "failed_validations": 0,
                "failed_expectations": 1
            },
            "kpi_revenue": {
                "success_rate": 100.0,
                "failed_validations": 0,
                "failed_expectations": 0
            }
        }
    },
    "notification_channels": {
        "email": {
            "enabled": True,
            "smtp_server": "smtp.example.com",
            "smtp_port": 587,
            "smtp_username": "alerts@example.com",
            "smtp_password": "password",
            "from_address": "alerts@example.com",
            "to_addresses": ["data_team@example.com"],
            "cc_addresses": [],
            "subject_prefix": "[DATA QUALITY ALERT]"
        },
        "slack": {
            "enabled": True,
            "webhook_url": "https://hooks.slack.com/services/TXXXXXXXX/BXXXXXXXX/XXXXXXXXXXXXXXXXXXXXXXXX",
            "channel": "#data-quality-alerts",
            "username": "Data Quality Bot",
            "icon_emoji": ":warning:"
        },
        "webhook": {
            "enabled": False,
            "url": "https://example.com/webhook",
            "headers": {
                "Content-Type": "application/json",
                "Authorization": "Bearer XXXXXXXXXXXXXXXXXXXXXXXX"
            }
        }
    },
    "alert_severity_levels": {
        "critical": {
            "threshold": 95.0,  # Threshold breach percentage for critical severity
            "escalation": True,
            "escalation_delay": 0,  # Immediate escalation
            "notification_channels": ["email", "slack"]
        },
        "high": {
            "threshold": 90.0,
            "escalation": True,
            "escalation_delay": 30,  # 30 minutes
            "notification_channels": ["email", "slack"]
        },
        "medium": {
            "threshold": 80.0,
            "escalation": False,
            "notification_channels": ["slack"]
        },
        "low": {
            "threshold": 70.0,
            "escalation": False,
            "notification_channels": ["slack"]
        }
    },
    "escalation_workflow": {
        "levels": [
            {
                "level": 1,
                "delay": 0,  # Immediate notification
                "contacts": ["data_engineer@example.com"]
            },
            {
                "level": 2,
                "delay": 30,  # 30 minutes after level 1
                "contacts": ["data_lead@example.com"]
            },
            {
                "level": 3,
                "delay": 60,  # 60 minutes after level 2
                "contacts": ["cto@example.com"]
            }
        ]
    },
    "alert_history": {
        "retention_days": 90,  # Number of days to keep alert history
        "max_entries": 1000  # Maximum number of alert history entries to keep
    }
}

class AlertManager:
    """Class for managing data quality alerts."""
    
    def __init__(self, config_path=None):
        """
        Initialize the AlertManager.
        
        Args:
            config_path: Path to configuration file (optional)
        """
        self.config = self._load_config(config_path)
        self.alert_history = self._load_alert_history()
    
    def _load_config(self, config_path=None):
        """
        Load configuration from file or use default.
        
        Args:
            config_path: Path to configuration file (optional)
        
        Returns:
            dict: Configuration dictionary
        """
        if config_path is None:
            config_path = os.path.join(CONFIG_DIR, "alert_config.json")
        
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
    
    def _load_alert_history(self):
        """
        Load alert history from file.
        
        Returns:
            list: Alert history
        """
        history_path = os.path.join(HISTORY_DIR, "alert_history.json")
        
        if not os.path.exists(history_path):
            return []
        
        try:
            with open(history_path, "r") as f:
                history = json.load(f)
            logger.info(f"Loaded alert history from: {history_path}")
            return history
        except Exception as e:
            logger.error(f"Error loading alert history: {str(e)}")
            return []
    
    def _save_alert_history(self):
        """Save alert history to file."""
        history_path = os.path.join(HISTORY_DIR, "alert_history.json")
        
        try:
            # Prune history if needed
            max_entries = self.config["alert_history"]["max_entries"]
            retention_days = self.config["alert_history"]["retention_days"]
            
            if len(self.alert_history) > max_entries:
                self.alert_history = self.alert_history[-max_entries:]
            
            if retention_days > 0:
                cutoff_date = (datetime.datetime.now() - datetime.timedelta(days=retention_days)).isoformat()
                self.alert_history = [
                    alert for alert in self.alert_history
                    if alert.get("timestamp", "0") >= cutoff_date
                ]
            
            with open(history_path, "w") as f:
                json.dump(self.alert_history, f, indent=2)
            
            logger.info(f"Saved alert history to: {history_path}")
        except Exception as e:
            logger.error(f"Error saving alert history: {str(e)}")
    
    def check_thresholds(self, validation_summary):
        """
        Check if validation results breach any thresholds.
        
        Args:
            validation_summary: Validation summary dictionary
        
        Returns:
            list: List of threshold breaches
        """
        logger.info("Checking validation results against thresholds")
        
        breaches = []
        
        # Check global thresholds
        global_thresholds = self.config["alert_thresholds"]["global"]
        
        if validation_summary["success_rate"] < global_thresholds["success_rate"]:
            breaches.append({
                "level": "global",
                "type": "success_rate",
                "threshold": global_thresholds["success_rate"],
                "actual": validation_summary["success_rate"],
                "description": f"Global success rate below threshold: {validation_summary['success_rate']:.2f}% < {global_thresholds['success_rate']:.2f}%"
            })
        
        if validation_summary["failed_validations"] > global_thresholds["failed_validations"]:
            breaches.append({
                "level": "global",
                "type": "failed_validations",
                "threshold": global_thresholds["failed_validations"],
                "actual": validation_summary["failed_validations"],
                "description": f"Too many failed validations: {validation_summary['failed_validations']} > {global_thresholds['failed_validations']}"
            })
        
        # Check layer thresholds
        for layer_name, layer_data in validation_summary["layers"].items():
            if layer_name in self.config["alert_thresholds"]:
                layer_thresholds = self.config["alert_thresholds"][layer_name]
                
                if layer_data["rate"] < layer_thresholds["success_rate"]:
                    breaches.append({
                        "level": "layer",
                        "layer": layer_name,
                        "type": "success_rate",
                        "threshold": layer_thresholds["success_rate"],
                        "actual": layer_data["rate"],
                        "description": f"{layer_name.capitalize()} layer success rate below threshold: {layer_data['rate']:.2f}% < {layer_thresholds['success_rate']:.2f}%"
                    })
                
                if layer_data["failure"] > layer_thresholds["failed_validations"]:
                    breaches.append({
                        "level": "layer",
                        "layer": layer_name,
                        "type": "failed_validations",
                        "threshold": layer_thresholds["failed_validations"],
                        "actual": layer_data["failure"],
                        "description": f"Too many failed validations in {layer_name} layer: {layer_data['failure']} > {layer_thresholds['failed_validations']}"
                    })
        
        # Check dataset thresholds
        for dataset_name, dataset_data in validation_summary["datasets"].items():
            if dataset_name in self.config["alert_thresholds"]["datasets"]:
                dataset_thresholds = self.config["alert_thresholds"]["datasets"][dataset_name]
                
                if dataset_data["success_rate"] < dataset_thresholds["success_rate"]:
                    breaches.append({
                        "level": "dataset",
                        "dataset": dataset_name,
                        "layer": dataset_data["layer"],
                        "type": "success_rate",
                        "threshold": dataset_thresholds["success_rate"],
                        "actual": dataset_data["success_rate"],
                        "description": f"Dataset {dataset_name} success rate below threshold: {dataset_data['success_rate']:.2f}% < {dataset_thresholds['success_rate']:.2f}%"
                    })
                
                if not dataset_data["success"] and dataset_thresholds["failed_validations"] == 0:
                    breaches.append({
                        "level": "dataset",
                        "dataset": dataset_name,
                        "layer": dataset_data["layer"],
                        "type": "failed_validation",
                        "threshold": dataset_thresholds["failed_validations"],
                        "actual": 1,
                        "description": f"Dataset {dataset_name} validation failed (zero tolerance)"
                    })
                
                if dataset_data["failed_expectations"] > dataset_thresholds["failed_expectations"]:
                    breaches.append({
                        "level": "dataset",
                        "dataset": dataset_name,
                        "layer": dataset_data["layer"],
                        "type": "failed_expectations",
                        "threshold": dataset_thresholds["failed_expectations"],
                        "actual": dataset_data["failed_expectations"],
                        "description": f"Too many failed expectations in dataset {dataset_name}: {dataset_data['failed_expectations']} > {dataset_thresholds['failed_expectations']}"
                    })
        
        if breaches:
            logger.info(f"Found {len(breaches)} threshold breaches")
        else:
            logger.info("No threshold breaches found")
        
        return breaches
    
    def determine_severity(self, breaches):
        """
        Determine the severity level of threshold breaches.
        
        Args:
            breaches: List of threshold breaches
        
        Returns:
            str: Severity level (critical, high, medium, low)
        """
        if not breaches:
            return None
        
        # Calculate breach percentage
        breach_percentage = 0
        
        for breach in breaches:
            if breach["type"] == "success_rate":
                # For success rate breaches, calculate how far below threshold
                threshold = breach["threshold"]
                actual = breach["actual"]
                percentage_below = ((threshold - actual) / threshold) * 100
                breach_percentage = max(breach_percentage, percentage_below)
        
        # Determine severity based on breach percentage
        severity_levels = sel
(Content truncated due to size limit. Use line ranges to read in chunks)