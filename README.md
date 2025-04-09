# E-Commerce Data Pipeline

A scalable, end-to-end data pipeline for processing e-commerce data, transforming it into actionable insights, and visualizing key performance indicators.

## Overview

This project implements a comprehensive data engineering solution for a mock e-commerce platform. It handles data from multiple sources, processes it through a lakehouse architecture, and provides visualizations of key business metrics.

### Features

- **Data Generation**: Mock data generation for user activity, orders, and inventory
- **Data Ingestion**: Streaming and batch ingestion with schema validation
- **Data Processing**: Cleaning, normalization, and transformation using Apache Spark
- **Data Storage**: Lakehouse model with bronze/silver/gold layers
- **Orchestration**: Workflow management with Airflow and Prefect
- **Monitoring**: Data quality checks, pipeline health monitoring, and anomaly detection
- **Visualization**: Interactive dashboard with Streamlit

## Architecture

The pipeline follows a modern data engineering architecture with distinct layers:

```
Data Sources → Ingestion → Processing → Storage → Visualization
                   ↑           ↑           ↑
                   └───── Orchestration ────┘
                   └───── Monitoring ───────┘
```

For a detailed architecture diagram and component descriptions, see the [documentation](docs/documentation.md).

## Directory Structure

```
ecommerce_data_pipeline/
├── data_generation/       # Data generation scripts
├── data_ingestion/        # Data ingestion modules
├── data_processing/       # Data processing and transformation
├── data_storage/          # Data storage implementation
├── orchestration/         # Workflow orchestration
├── monitoring/            # Monitoring and logging
├── visualization/         # Data visualization
├── docs/                  # Documentation
├── data/                  # Data directory
│   ├── bronze/            # Raw data
│   ├── silver/            # Cleaned data
│   └── gold/              # Business data
└── logs/                  # Log files
```

## Getting Started

### Prerequisites

- Python 3.10 or higher
- pip (Python package installer)
- Virtual environment tool (venv)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/ecommerce_data_pipeline.git
   cd ecommerce_data_pipeline
   ```

2. Create and activate a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up directory structure:
   ```bash
   mkdir -p data/{bronze,silver,gold/{dimensions,facts,kpis}} logs
   ```

### Running the Pipeline

1. Generate sample data:
   ```bash
   python -m data_generation.user_activity_generator
   python -m data_generation.orders_generator
   python -m data_generation.inventory_generator
   ```

2. Run the ingestion process:
   ```bash
   python -m data_ingestion.ingestion_module
   ```

3. Run the processing and transformation:
   ```bash
   python -m data_processing.transformation_module
   ```

4. Start the dashboard:
   ```bash
   streamlit run visualization/streamlit_dashboard.py
   ```

For detailed instructions, see the [setup guide](docs/documentation.md#setup-guide).

## Dashboard

The Streamlit dashboard provides insights into the e-commerce data, including:

- Revenue metrics and trends
- Top-selling products and categories
- User retention analysis
- Data quality metrics
- Pipeline health monitoring

To access the dashboard, start it with the command above and navigate to `http://localhost:8501` in your web browser.

## Documentation

Comprehensive documentation is available in the `docs/` directory:

- [Full Documentation](docs/documentation.md): Complete project documentation
- [Architecture](docs/documentation.md#architecture-overview): Detailed architecture description
- [Setup Guide](docs/documentation.md#setup-guide): Installation and configuration instructions
- [User Manual](docs/documentation.md#user-manual): Dashboard usage guide

## Technologies Used

- **Python**: Primary programming language
- **Apache Spark**: Distributed data processing
- **Pandas**: Data manipulation and analysis
- **Parquet**: Columnar storage format
- **Airflow/Prefect**: Workflow orchestration
- **Streamlit**: Data visualization and dashboard

## Project Requirements

This project was developed to meet the following requirements:

1. **Data Sources**:
   - User activity data (JSON format, streaming)
   - Orders data (CSV format, daily batch)
   - Inventory data (XLSX format, weekly batch)

2. **Data Processing**:
   - Data cleaning and normalization
   - Handling nulls, duplicates, and type mismatches
   - Joining datasets
   - Computing KPIs (total revenue, top products, user retention)

3. **Architecture**:
   - Lakehouse model with bronze/silver/gold layers
   - Scalable and maintainable design
   - Comprehensive monitoring and logging

4. **Visualization**:
   - Interactive dashboard for KPIs
   - Data quality metrics
   - Pipeline health monitoring

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- This project was created as a demonstration of data engineering best practices
- Inspired by real-world e-commerce data processing challenges
