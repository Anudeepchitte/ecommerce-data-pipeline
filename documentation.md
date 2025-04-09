# E-Commerce Data Pipeline Documentation

## Architecture Overview

The e-commerce data pipeline is designed as a scalable, end-to-end solution for processing data from multiple sources, transforming it into actionable insights, and visualizing key performance indicators. The architecture follows modern data engineering principles with a lakehouse model that combines the best aspects of data lakes and data warehouses.

### Architecture Diagram

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Data Sources   │     │  Data Ingestion │     │ Data Processing │
│                 │     │                 │     │                 │
│ ┌─────────────┐ │     │ ┌─────────────┐ │     │ ┌─────────────┐ │
│ │User Activity│─┼────▶│ │Stream       │─┼────▶│ │Cleaning     │ │
│ │(JSON)       │ │     │ │Ingestion    │ │     │ │Normalization│ │
│ └─────────────┘ │     │ └─────────────┘ │     │ └─────────────┘ │
│                 │     │                 │     │                 │
│ ┌─────────────┐ │     │ ┌─────────────┐ │     │ ┌─────────────┐ │
│ │Orders       │─┼────▶│ │Batch        │─┼────▶│ │Joining      │ │
│ │(CSV)        │ │     │ │Ingestion    │ │     │ │Transformation│ │
│ └─────────────┘ │     │ └─────────────┘ │     │ └─────────────┘ │
│                 │     │                 │     │                 │
│ ┌─────────────┐ │     │ ┌─────────────┐ │     │ ┌─────────────┐ │
│ │Inventory    │─┼────▶│ │Schema       │─┼────▶│ │KPI          │ │
│ │(XLSX)       │ │     │ │Validation   │ │     │ │Calculation  │ │
│ └─────────────┘ │     │ └─────────────┘ │     │ └─────────────┘ │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                      │                       │
         ▼                      ▼                       ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Data Storage   │     │  Orchestration  │     │  Monitoring     │
│                 │     │                 │     │                 │
│ ┌─────────────┐ │     │ ┌─────────────┐ │     │ ┌─────────────┐ │
│ │Bronze Layer │ │     │ │Airflow DAGs │ │     │ │Data Quality │ │
│ │(Raw Data)   │ │     │ │             │ │     │ │Monitoring   │ │
│ └─────────────┘ │     │ └─────────────┘ │     │ └─────────────┘ │
│                 │     │                 │     │                 │
│ ┌─────────────┐ │     │ ┌─────────────┐ │     │ ┌─────────────┐ │
│ │Silver Layer │ │     │ │Prefect Flows│ │     │ │Pipeline     │ │
│ │(Cleaned)    │ │     │ │             │ │     │ │Health       │ │
│ └─────────────┘ │     │ └─────────────┘ │     │ └─────────────┘ │
│                 │     │                 │     │                 │
│ ┌─────────────┐ │     │ ┌─────────────┐ │     │ ┌─────────────┐ │
│ │Gold Layer   │ │     │ │Error        │ │     │ │Anomaly      │ │
│ │(Business)   │ │     │ │Handling     │ │     │ │Detection    │ │
│ └─────────────┘ │     │ └─────────────┘ │     │ └─────────────┘ │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                      │                       │
         └──────────────────────┼───────────────────────┘
                                ▼
                      ┌─────────────────┐
                      │  Visualization  │
                      │                 │
                      │ ┌─────────────┐ │
                      │ │Streamlit    │ │
                      │ │Dashboard    │ │
                      │ └─────────────┘ │
                      │                 │
                      │ ┌─────────────┐ │
                      │ │KPI          │ │
                      │ │Visualizations│ │
                      │ └─────────────┘ │
                      │                 │
                      │ ┌─────────────┐ │
                      │ │Monitoring   │ │
                      │ │Dashboards   │ │
                      │ └─────────────┘ │
                      └─────────────────┘
```

### Data Flow

1. **Data Generation**: Mock data is generated to simulate real-world e-commerce data sources:
   - User activity data (JSON format) - simulating streaming data
   - Orders data (CSV format) - simulating daily batch data
   - Inventory data (XLSX format) - simulating weekly batch data

2. **Data Ingestion**: Data is ingested from various sources with appropriate validation:
   - Streaming ingestion for user activity data
   - Batch ingestion for orders and inventory data
   - Schema validation and evolution handling
   - Error handling for bad data

3. **Data Processing & Transformation**: Data is cleaned, normalized, and transformed:
   - Data cleaning and normalization
   - Handling nulls, duplicates, and type mismatches
   - Joining datasets
   - Computing KPIs (total revenue, top products, user retention)

4. **Data Storage**: Data is stored in a lakehouse model with three layers:
   - Bronze layer: Raw data as ingested
   - Silver layer: Cleaned and normalized data
   - Gold layer: Business-ready data (dimensions, facts, KPIs)

5. **Orchestration**: Pipeline execution is orchestrated with:
   - Airflow DAGs for daily and weekly processing
   - Prefect flows for task dependencies
   - Error handling and retry logic
   - Failure notifications

6. **Monitoring & Logging**: Pipeline health and data quality are monitored:
   - Comprehensive logging system
   - Data quality monitoring
   - Pipeline health checks
   - Anomaly detection
   - Alerting system

7. **Visualization**: Insights are visualized through:
   - Streamlit dashboard for KPIs
   - Data quality metrics visualization
   - Pipeline health monitoring
   - Interactive features for data exploration

## Technology Stack

### Core Technologies

- **Python**: Primary programming language
- **Apache Spark**: Distributed data processing
- **Pandas**: Data manipulation and analysis
- **Parquet**: Columnar storage format
- **Streamlit**: Data visualization and dashboard

### Data Processing

- **PySpark**: Python API for Apache Spark
- **NumPy**: Numerical computing
- **Scikit-learn**: Machine learning for anomaly detection

### Orchestration

- **Apache Airflow**: Workflow orchestration
- **Prefect**: Dataflow automation

### Visualization

- **Plotly**: Interactive visualizations
- **Matplotlib/Seaborn**: Static visualizations

### Development Tools

- **Virtual Environment**: Dependency isolation
- **Logging**: Comprehensive logging framework
- **JSON/CSV/XLSX**: Data formats

## Folder Structure

```
ecommerce_data_pipeline/
│
├── data_generation/               # Data generation scripts
│   ├── user_activity_generator.py # Generates user activity data (JSON)
│   ├── orders_generator.py        # Generates orders data (CSV)
│   └── inventory_generator.py     # Generates inventory data (XLSX)
│
├── data_ingestion/                # Data ingestion modules
│   ├── ingestion_module.py        # General ingestion functionality
│   ├── streaming_ingestion.py     # Streaming ingestion for user activity
│   └── schema_registry.py         # Schema validation and evolution
│
├── data_processing/               # Data processing and transformation
│   └── transformation_module.py   # Data cleaning, joining, and KPI calculation
│
├── data_storage/                  # Data storage implementation
│   ├── storage_module.py          # Lakehouse model implementation
│   └── storage_optimization.py    # Storage optimization utilities
│
├── orchestration/                 # Workflow orchestration
│   ├── airflow_dags.py            # Airflow DAG definitions
│   └── prefect_flows.py           # Prefect flow definitions
│
├── monitoring/                    # Monitoring and logging
│   └── monitoring_module.py       # Data quality and pipeline monitoring
│
├── visualization/                 # Data visualization
│   └── streamlit_dashboard.py     # Streamlit dashboard implementation
│
├── docs/                          # Documentation
│   ├── architecture.md            # Architecture documentation
│   ├── setup_guide.md             # Setup and installation guide
│   └── user_manual.md             # User manual for the dashboard
│
├── data/                          # Data directory
│   ├── bronze/                    # Bronze layer (raw data)
│   ├── silver/                    # Silver layer (cleaned data)
│   └── gold/                      # Gold layer (business data)
│       ├── dimensions/            # Dimension tables
│       ├── facts/                 # Fact tables
│       └── kpis/                  # KPI data
│
├── logs/                          # Log files
│
├── venv/                          # Virtual environment
│
└── README.md                      # Project overview
```

## Component Details

### Data Generation

The data generation component creates realistic mock data for testing and demonstration purposes:

- **User Activity Generator**: Creates user browsing and interaction events in JSON format, simulating streaming data.
- **Orders Generator**: Produces order records in CSV format, including customer information, products ordered, and order details.
- **Inventory Generator**: Generates product inventory data in XLSX format, including stock levels, pricing, and product details.

### Data Ingestion

The data ingestion layer handles data from various sources with appropriate validation:

- **Ingestion Module**: Core functionality for ingesting data from different sources.
- **Streaming Ingestion**: Specialized module for handling streaming user activity data.
- **Schema Registry**: Manages schema validation and evolution, ensuring data consistency.

### Data Processing & Transformation

The processing layer cleans, normalizes, and transforms data:

- **Transformation Module**: Implements data cleaning, normalization, joining, and KPI calculations using Apache Spark.
- **Data Cleaning**: Handles nulls, duplicates, and type mismatches.
- **Data Joining**: Combines data from different sources.
- **KPI Calculation**: Computes business metrics like total revenue, top products, and user retention.

### Data Storage

The storage layer implements a lakehouse model with three layers:

- **Bronze Layer**: Raw data as ingested from sources.
- **Silver Layer**: Cleaned and normalized data.
- **Gold Layer**: Business-ready data, including dimension tables, fact tables, and KPI data.
- **Storage Optimization**: Utilities for optimizing storage, including partitioning and compaction.

### Orchestration

The orchestration layer manages workflow execution:

- **Airflow DAGs**: Defines daily and weekly processing workflows.
- **Prefect Flows**: Implements task dependencies and execution flow.
- **Error Handling**: Includes retry logic and failure notifications.

### Monitoring & Logging

The monitoring layer tracks pipeline health and data quality:

- **Data Quality Monitoring**: Checks data completeness, uniqueness, and consistency.
- **Pipeline Health Monitoring**: Tracks processing time, error rates, and resource usage.
- **Anomaly Detection**: Identifies unusual patterns in data.
- **Alerting System**: Sends notifications for critical issues.

### Visualization

The visualization layer presents insights through interactive dashboards:

- **Streamlit Dashboard**: Interactive web application for data exploration.
- **KPI Visualizations**: Charts and graphs for key business metrics.
- **Monitoring Dashboards**: Visualizations for data quality and pipeline health.

## Setup Guide

### Prerequisites

- Python 3.10 or higher
- pip (Python package installer)
- Virtual environment tool (venv)

### Installation Steps

1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/ecommerce_data_pipeline.git
   cd ecommerce_data_pipeline
   ```

2. **Create and activate a virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up directory structure**:
   ```bash
   mkdir -p data/{bronze,silver,gold/{dimensions,facts,kpis}} logs
   ```

5. **Install additional components** (if needed):
   - For Airflow: `pip install apache-airflow`
   - For Prefect: `pip install prefect`

### Configuration

1. **Environment Variables**:
   Create a `.env` file with the following variables:
   ```
   DATA_DIR=/path/to/data
   LOG_LEVEL=INFO
   ```

2. **Spark Configuration**:
   Adjust Spark memory settings in the processing modules based on your environment.

### Running the Pipeline

1. **Generate sample data**:
   ```bash
   python -m data_generation.user_activity_generator
   python -m data_generation.orders_generator
   python -m data_generation.inventory_generator
   ```

2. **Run the ingestion process**:
   ```bash
   python -m data_ingestion.ingestion_module
   ```

3. **Run the processing and transformation**:
   ```bash
   python -m data_processing.transformation_module
   ```

4. **Start the dashboard**:
   ```bash
   streamlit run visualization/streamlit_dashboard.py
   ```

5. **For orchestration**:
   - With Airflow: `airflow scheduler` and `airflow webserver`
   - With Prefect: `prefect orion start` and `prefect agent start`

## User Manual

### Dashboard Overview

The Streamlit dashboard provides insights into the e-commerce data pipeline, including KPIs, data quality metrics, and pipeline health monitoring.

#### Accessing the Dashboard

1. Start the dashboard:
   ```bash
   streamlit run visualization/streamlit_dashboard.py
   ```

2. Open your web browser and navigate to:
   ```
   http://localhost:8501
   ```

#### Dashboard Sections

1. **KPIs Tab**:
   - Total Revenue: Overall revenue metrics
   - Daily Revenue Trend: Revenue over time
   - Top Products: Highest revenue-generating products
   - Category Breakdown: Revenue by product category
   - User Retention: Customer retention by cohort

2. **Data Quality Tab**:
   - Data Completeness: Percentage of non-null values
   - Data Uniqueness: Percentage of unique values
   - Data Validity: Percentage of valid values
   - Data Consistency: Consistency across related fields
   - Anomaly Detection: Unusual patterns in data

3. **Pipeline Health Tab**:
   - Processing Time: Time taken for pipeline execution
   - Error Count: Number of errors in pipeline execution
   - Storage Size: Size of data in each layer
   - Pipeline Status: Overall health status

#### Using the Dashboard

1. **Date Range Selection**:
   - Use the date range selector to filter data by time period

2. **Interactive Features**:
   - Hover over charts for detailed information
   - Click on legend items to filter data
   - Use dropdown menus to select different metrics

3. **Data Refresh**:
   - Click the refresh button to update data
   - Set automatic refresh interval in settings

## Maintenance and Troubleshooting

### Common Issues

1. **Pipeline Failures**:
   - Check logs in the `logs/` directory
   - Verify data source availability
   - Ensure sufficient disk space

2. **Dashboard Performance**:
   - Optimize data queries
   - Increase server resources
   - Implement data caching

3. **Data Quality Issues**:
   - Review data quality monitoring alerts
   - Check source data for anomalies
   - Verify transformation logic

### Maintenance Tasks

1. **Regular Backups**:
   - Back up the `data/gold` directory regularly

2. **Log Rotation**:
   - Implement log rotation to manage log file size

3. **Performance Optimization**:
   - Monitor and optimize storage partitioning
   - Compact small files periodically
   - Tune Spark parameters based on workload

## Conclusion

This e-commerce data pipeline provides a scalable, end-to-end solution for processing e-commerce data and deriving actionable insights. The modular architecture allows for easy extension and customization to meet specific business requirements.

For additional support or feature requests, please contact the development team.
