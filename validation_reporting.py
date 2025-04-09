#!/usr/bin/env python3
"""
Validation Reporting Module for E-commerce Data Pipeline

This module implements a reporting system for data validation results using Great Expectations.
It generates HTML reports, stores validation results, and provides a dashboard for monitoring
data quality across the pipeline.

Features:
- Validation result storage
- HTML report generation
- Validation dashboard
- Historical validation tracking
- Failure notification
"""

import os
import sys
import logging
import json
import datetime
import pandas as pd
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier
from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.view import DefaultJinjaPageView
from jinja2 import Environment, FileSystemLoader
import shutil

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import GE setup module
from data_validation.ge_setup import create_data_context, configure_spark_datasource

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/validation_reporting.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("validation_reporting")

# Create logs directory if it doesn't exist
os.makedirs("../logs", exist_ok=True)

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
GE_DIR = os.path.join(BASE_DIR, "data_validation", "great_expectations")
REPORTS_DIR = os.path.join(BASE_DIR, "data_validation", "reports")
TEMPLATES_DIR = os.path.join(BASE_DIR, "data_validation", "templates")

# Create directories if they don't exist
os.makedirs(REPORTS_DIR, exist_ok=True)
os.makedirs(os.path.join(REPORTS_DIR, "daily"), exist_ok=True)
os.makedirs(os.path.join(REPORTS_DIR, "weekly"), exist_ok=True)
os.makedirs(os.path.join(REPORTS_DIR, "monthly"), exist_ok=True)
os.makedirs(TEMPLATES_DIR, exist_ok=True)

class ValidationReporter:
    """Class for generating and managing validation reports."""
    
    def __init__(self, context):
        """
        Initialize the ValidationReporter.
        
        Args:
            context: Great Expectations data context
        """
        self.context = context
        self.validation_results = {}
        self.summary = {
            "timestamp": datetime.datetime.now().isoformat(),
            "total_validations": 0,
            "successful_validations": 0,
            "failed_validations": 0,
            "success_rate": 0.0,
            "layers": {
                "bronze": {"total": 0, "success": 0, "failure": 0, "rate": 0.0},
                "silver": {"total": 0, "success": 0, "failure": 0, "rate": 0.0},
                "gold": {"total": 0, "success": 0, "failure": 0, "rate": 0.0}
            },
            "datasets": {}
        }
        
        # Create Jinja2 environment for custom templates
        self.jinja_env = Environment(
            loader=FileSystemLoader(TEMPLATES_DIR)
        )
        
        # Create default templates if they don't exist
        self._create_default_templates()
    
    def _create_default_templates(self):
        """Create default Jinja2 templates for reports if they don't exist."""
        # Create summary template
        summary_template_path = os.path.join(TEMPLATES_DIR, "summary_report.html")
        if not os.path.exists(summary_template_path):
            with open(summary_template_path, "w") as f:
                f.write("""<!DOCTYPE html>
<html>
<head>
    <title>E-commerce Data Pipeline Validation Summary</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            color: #333;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        .header {
            background-color: #f8f9fa;
            padding: 20px;
            border-radius: 5px;
            margin-bottom: 20px;
            border-left: 5px solid #007bff;
        }
        .summary-box {
            display: flex;
            justify-content: space-between;
            margin-bottom: 20px;
        }
        .summary-item {
            flex: 1;
            padding: 15px;
            border-radius: 5px;
            text-align: center;
            margin: 0 10px;
        }
        .success {
            background-color: #d4edda;
            border-left: 5px solid #28a745;
        }
        .failure {
            background-color: #f8d7da;
            border-left: 5px solid #dc3545;
        }
        .warning {
            background-color: #fff3cd;
            border-left: 5px solid #ffc107;
        }
        .info {
            background-color: #d1ecf1;
            border-left: 5px solid #17a2b8;
        }
        .layer-summary {
            margin-bottom: 30px;
        }
        .dataset-table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 30px;
        }
        .dataset-table th, .dataset-table td {
            padding: 12px 15px;
            border: 1px solid #ddd;
            text-align: left;
        }
        .dataset-table th {
            background-color: #f8f9fa;
        }
        .dataset-table tr:nth-child(even) {
            background-color: #f2f2f2;
        }
        .dataset-table tr:hover {
            background-color: #e9ecef;
        }
        .progress-bar {
            height: 20px;
            background-color: #e9ecef;
            border-radius: 5px;
            margin-bottom: 10px;
        }
        .progress {
            height: 100%;
            border-radius: 5px;
            background-color: #007bff;
        }
        .footer {
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
            text-align: center;
            font-size: 0.9em;
            color: #6c757d;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>E-commerce Data Pipeline Validation Summary</h1>
            <p>Report generated on: {{ summary.timestamp }}</p>
        </div>
        
        <div class="summary-box">
            <div class="summary-item info">
                <h2>Total Validations</h2>
                <p>{{ summary.total_validations }}</p>
            </div>
            <div class="summary-item success">
                <h2>Successful</h2>
                <p>{{ summary.successful_validations }}</p>
            </div>
            <div class="summary-item failure">
                <h2>Failed</h2>
                <p>{{ summary.failed_validations }}</p>
            </div>
            <div class="summary-item {% if summary.success_rate >= 90 %}success{% elif summary.success_rate >= 70 %}warning{% else %}failure{% endif %}">
                <h2>Success Rate</h2>
                <p>{{ "%.2f"|format(summary.success_rate) }}%</p>
                <div class="progress-bar">
                    <div class="progress" style="width: {{ summary.success_rate }}%;"></div>
                </div>
            </div>
        </div>
        
        <h2>Layer Summary</h2>
        <div class="layer-summary">
            {% for layer_name, layer_data in summary.layers.items() %}
            <h3>{{ layer_name|capitalize }} Layer</h3>
            <div class="summary-box">
                <div class="summary-item info">
                    <h4>Total Validations</h4>
                    <p>{{ layer_data.total }}</p>
                </div>
                <div class="summary-item success">
                    <h4>Successful</h4>
                    <p>{{ layer_data.success }}</p>
                </div>
                <div class="summary-item failure">
                    <h4>Failed</h4>
                    <p>{{ layer_data.failure }}</p>
                </div>
                <div class="summary-item {% if layer_data.rate >= 90 %}success{% elif layer_data.rate >= 70 %}warning{% else %}failure{% endif %}">
                    <h4>Success Rate</h4>
                    <p>{{ "%.2f"|format(layer_data.rate) }}%</p>
                    <div class="progress-bar">
                        <div class="progress" style="width: {{ layer_data.rate }}%;"></div>
                    </div>
                </div>
            </div>
            {% endfor %}
        </div>
        
        <h2>Dataset Summary</h2>
        <table class="dataset-table">
            <thead>
                <tr>
                    <th>Dataset</th>
                    <th>Layer</th>
                    <th>Status</th>
                    <th>Expectations</th>
                    <th>Successful</th>
                    <th>Failed</th>
                    <th>Success Rate</th>
                    <th>Report</th>
                </tr>
            </thead>
            <tbody>
                {% for dataset_name, dataset_data in summary.datasets.items() %}
                <tr>
                    <td>{{ dataset_name }}</td>
                    <td>{{ dataset_data.layer }}</td>
                    <td class="{% if dataset_data.success %}success{% else %}failure{% endif %}">
                        {{ "Success" if dataset_data.success else "Failure" }}
                    </td>
                    <td>{{ dataset_data.total_expectations }}</td>
                    <td>{{ dataset_data.successful_expectations }}</td>
                    <td>{{ dataset_data.failed_expectations }}</td>
                    <td>
                        {{ "%.2f"|format(dataset_data.success_rate) }}%
                        <div class="progress-bar">
                            <div class="progress" style="width: {{ dataset_data.success_rate }}%;"></div>
                        </div>
                    </td>
                    <td><a href="{{ dataset_data.report_url }}" target="_blank">View Report</a></td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        
        <div class="footer">
            <p>E-commerce Data Pipeline Validation Report &copy; {{ summary.timestamp.split('T')[0].split('-')[0] }}</p>
        </div>
    </div>
</body>
</html>""")
        
        # Create daily report template
        daily_template_path = os.path.join(TEMPLATES_DIR, "daily_report.html")
        if not os.path.exists(daily_template_path):
            with open(daily_template_path, "w") as f:
                f.write("""<!DOCTYPE html>
<html>
<head>
    <title>E-commerce Data Pipeline Daily Validation Report</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            color: #333;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        .header {
            background-color: #f8f9fa;
            padding: 20px;
            border-radius: 5px;
            margin-bottom: 20px;
            border-left: 5px solid #007bff;
        }
        .summary-box {
            display: flex;
            justify-content: space-between;
            margin-bottom: 20px;
        }
        .summary-item {
            flex: 1;
            padding: 15px;
            border-radius: 5px;
            text-align: center;
            margin: 0 10px;
        }
        .success {
            background-color: #d4edda;
            border-left: 5px solid #28a745;
        }
        .failure {
            background-color: #f8d7da;
            border-left: 5px solid #dc3545;
        }
        .warning {
            background-color: #fff3cd;
            border-left: 5px solid #ffc107;
        }
        .info {
            background-color: #d1ecf1;
            border-left: 5px solid #17a2b8;
        }
        .dataset-table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 30px;
        }
        .dataset-table th, .dataset-table td {
            padding: 12px 15px;
            border: 1px solid #ddd;
            text-align: left;
        }
        .dataset-table th {
            background-color: #f8f9fa;
        }
        .dataset-table tr:nth-child(even) {
            background-color: #f2f2f2;
        }
        .dataset-table tr:hover {
            background-color: #e9ecef;
        }
        .progress-bar {
            height: 20px;
            background-color: #e9ecef;
            border-radius: 5px;
            margin-bottom: 10px;
        }
        .progress {
            height: 100%;
            border-radius: 5px;
            background-color: #007bff;
        }
        .footer {
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
            text-align: center;
            font-size: 0.9em;
            color: #6c757d;
        }
        .trend-chart {
            width: 100%;
            height: 300px;
            margin-bottom: 30px;
            border: 1px solid #ddd;
            border-radius: 5px;
            padding: 10px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>E-commerce Data Pipeline Daily Validation Report</h1>
            <p>Report for: {{ report_date }}</p>
            <p>Generated on: {{ generation_timestamp }}</p>
        </div>
        
        <div class="summary-box">
            <div class="summary-item info">
                <h2>Total Validations</h2>
                <p>{{ summary.total_validations }}</p>
            </div>
            <div class="summary-item success">
                <h2>Successful</h2>
                <p>{{ summary.successful_validations }}</p>
            </div>
            <div class="summary-item failure">
                <h2>Failed</h2>
                <p>{{ summary.failed_validations }}</p>
            </div>
            <div class="summary-item {% if summary.success_rate >= 90 %}success{% elif summary.success_rate >= 70 %}warning{% else %}failure{% endif %}">
                <h2>Success Rate</h2>
                <p>{{ "%.2f"|format(summary.success_rate) }}%</p>
                <div class="progress-bar">
                    <div class="progress" style="width: {{ summary.success_rate }}%;"></div>
                </div>
            </div>
        </div>
        
        <h2>Dataset Summary</h2>
        <table class="dataset-table">
            <thead>
                <tr>
                    <th>Dataset</th>
                    <th>Layer</th>
                    <th>Status</th>
                    <th>Expectations</th>
                    <th>Successful</th>
                    <th>Failed</th>
                    <th>Success Rate</th>
                    <th>Report</th>
                </tr>
            </thead>
            <tbody>
                {% for dataset_name, dataset_data in summary.datasets.items() %}
                <tr>
                    <td>{{ dataset_name }}</td>
                    <td>{{ dataset_data.layer }}</td>
                    <td class="{% if dataset_data.success %}success{% else %}failure{% endif %}">
                        {{ "Success" if dataset_data.success else "Failure" }}
                    </td>
                    <td>{{ dataset_data.total_expectations }}</td>
                    <td>{{ dataset_data.successful_expectations }}</td>
                    <td>{{ dataset_data.failed_expectations }}</td>
                    <td>
                        {{ "%.2f"|format(dataset_data.success_rate) }}%
                        <div class="progress-bar">
                            <div class="progress" style="width: {{ data
(Content truncated due to size limit. Use line ranges to read in chunks)