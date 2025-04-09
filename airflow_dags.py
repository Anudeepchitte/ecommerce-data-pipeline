#!/usr/bin/env python3
"""
Airflow DAGs for E-commerce Data Pipeline

This module implements the orchestration framework for the data pipeline using Apache Airflow.
It defines DAGs for daily and weekly processing with dependencies and failure alerts.

Features:
- Daily processing DAG for user activity and orders
- Weekly processing DAG for inventory
- Dependency management
- Failure alerts and retry logic
"""

from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.email import send_email
from airflow.models import Variable

# Default arguments for DAGs
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email': ['data_alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Define paths
BASE_DIR = '/home/ubuntu/ecommerce_data_pipeline'
DATA_DIR = f'{BASE_DIR}/data'
SCRIPTS_DIR = BASE_DIR

# Add parent directory to path
sys.path.append(BASE_DIR)

# Import modules from other components
from data_generation.user_activity_generator import generate_user_activity, save_to_json
from data_generation.orders_generator import generate_orders, save_to_csv
from data_generation.inventory_generator import generate_inventory_data, save_to_excel

# Define Python callables for tasks
def generate_user_activity_data(**kwargs):
    """Generate user activity data."""
    num_users = 1000
    events_per_user_range = (5, 30)
    output_file = f"{DATA_DIR}/user_activity.json"
    
    # Create data directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Generate data
    start_date = datetime.now() - timedelta(days=1)  # Last day
    end_date = datetime.now()
    
    user_activities = generate_user_activity(
        num_users=num_users,
        events_per_user_range=events_per_user_range,
        start_date=start_date,
        end_date=end_date
    )
    
    # Save to file
    save_to_json(user_activities, output_file)
    print(f"Generated {len(user_activities)} user activity events")
    print(f"Data saved to {os.path.abspath(output_file)}")
    
    return output_file

def generate_orders_data(**kwargs):
    """Generate orders data."""
    num_orders = 500
    output_file = f"{DATA_DIR}/orders.csv"
    
    # Create data directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Generate data
    start_date = datetime.now() - timedelta(days=1)  # Last day
    end_date = datetime.now()
    
    orders = generate_orders(
        num_orders=num_orders,
        start_date=start_date,
        end_date=end_date
    )
    
    # Save to file
    save_to_csv(orders, output_file)
    print(f"Generated {len(orders)} orders")
    print(f"Data saved to {os.path.abspath(output_file)}")
    
    return output_file

def generate_inventory_data(**kwargs):
    """Generate inventory data."""
    num_products = 1000
    output_file = f"{DATA_DIR}/inventory.xlsx"
    
    # Create data directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Generate data
    as_of_date = datetime.now()
    
    inventory_data = generate_inventory_data(
        num_products=num_products,
        as_of_date=as_of_date
    )
    
    # Save to file
    save_to_excel(inventory_data, output_file)
    print(f"Generated {len(inventory_data)} inventory items")
    print(f"Data saved to {os.path.abspath(output_file)}")
    
    return output_file

def send_success_notification(context):
    """Send success notification."""
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    
    subject = f"Airflow Success: {dag_id}.{task_id} on {execution_date}"
    html_content = f"""
    <h3>Task {task_id} in DAG {dag_id} succeeded</h3>
    <p>Execution date: {execution_date}</p>
    <p>Task completed successfully.</p>
    """
    
    send_email(
        to=default_args['email'],
        subject=subject,
        html_content=html_content
    )

def send_failure_notification(context):
    """Send failure notification."""
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    exception = context.get('exception')
    
    subject = f"Airflow Alert: {dag_id}.{task_id} failed on {execution_date}"
    html_content = f"""
    <h3>Task {task_id} in DAG {dag_id} failed</h3>
    <p>Execution date: {execution_date}</p>
    <p>Exception: {exception}</p>
    """
    
    send_email(
        to=default_args['email'],
        subject=subject,
        html_content=html_content
    )

# Define DAGs
daily_dag = DAG(
    'ecommerce_daily_processing',
    default_args=default_args,
    description='Daily processing of user activity and orders data',
    schedule_interval='0 1 * * *',  # Run at 1 AM every day
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['ecommerce', 'daily'],
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
)

weekly_dag = DAG(
    'ecommerce_weekly_processing',
    default_args=default_args,
    description='Weekly processing of inventory data',
    schedule_interval='0 2 * * 0',  # Run at 2 AM every Sunday
    start_date=datetime(2025, 4, 6),  # First Sunday in April 2025
    catchup=False,
    tags=['ecommerce', 'weekly'],
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
)

# Define tasks for daily DAG
start_daily = DummyOperator(
    task_id='start_daily_processing',
    dag=daily_dag,
)

generate_user_activity = PythonOperator(
    task_id='generate_user_activity_data',
    python_callable=generate_user_activity_data,
    dag=daily_dag,
)

generate_orders = PythonOperator(
    task_id='generate_orders_data',
    python_callable=generate_orders_data,
    dag=daily_dag,
)

ingest_user_activity = BashOperator(
    task_id='ingest_user_activity_data',
    bash_command=f'cd {SCRIPTS_DIR} && python -m data_ingestion.ingestion_module',
    dag=daily_dag,
)

ingest_orders = BashOperator(
    task_id='ingest_orders_data',
    bash_command=f'cd {SCRIPTS_DIR} && python -m data_ingestion.ingestion_module',
    dag=daily_dag,
)

process_user_activity = BashOperator(
    task_id='process_user_activity_data',
    bash_command=f'cd {SCRIPTS_DIR} && python -m data_processing.transformation_module',
    dag=daily_dag,
)

process_orders = BashOperator(
    task_id='process_orders_data',
    bash_command=f'cd {SCRIPTS_DIR} && python -m data_processing.transformation_module',
    dag=daily_dag,
)

transform_to_gold = BashOperator(
    task_id='transform_to_gold_layer',
    bash_command=f'cd {SCRIPTS_DIR} && python -m data_processing.transformation_module',
    dag=daily_dag,
)

optimize_storage = BashOperator(
    task_id='optimize_storage',
    bash_command=f'cd {SCRIPTS_DIR} && python -m data_storage.storage_optimization',
    dag=daily_dag,
)

end_daily = DummyOperator(
    task_id='end_daily_processing',
    dag=daily_dag,
    trigger_rule='all_success',
)

# Define tasks for weekly DAG
start_weekly = DummyOperator(
    task_id='start_weekly_processing',
    dag=weekly_dag,
)

generate_inventory = PythonOperator(
    task_id='generate_inventory_data',
    python_callable=generate_inventory_data,
    dag=weekly_dag,
)

ingest_inventory = BashOperator(
    task_id='ingest_inventory_data',
    bash_command=f'cd {SCRIPTS_DIR} && python -m data_ingestion.ingestion_module',
    dag=weekly_dag,
)

process_inventory = BashOperator(
    task_id='process_inventory_data',
    bash_command=f'cd {SCRIPTS_DIR} && python -m data_processing.transformation_module',
    dag=weekly_dag,
)

transform_weekly_to_gold = BashOperator(
    task_id='transform_weekly_to_gold_layer',
    bash_command=f'cd {SCRIPTS_DIR} && python -m data_processing.transformation_module',
    dag=weekly_dag,
)

optimize_weekly_storage = BashOperator(
    task_id='optimize_weekly_storage',
    bash_command=f'cd {SCRIPTS_DIR} && python -m data_storage.storage_optimization',
    dag=weekly_dag,
)

end_weekly = DummyOperator(
    task_id='end_weekly_processing',
    dag=weekly_dag,
    trigger_rule='all_success',
)

# Define task dependencies for daily DAG
start_daily >> [generate_user_activity, generate_orders]
generate_user_activity >> ingest_user_activity
generate_orders >> ingest_orders
[ingest_user_activity, ingest_orders] >> [process_user_activity, process_orders]
[process_user_activity, process_orders] >> transform_to_gold
transform_to_gold >> optimize_storage
optimize_storage >> end_daily

# Define task dependencies for weekly DAG
start_weekly >> generate_inventory
generate_inventory >> ingest_inventory
ingest_inventory >> process_inventory
process_inventory >> transform_weekly_to_gold
transform_weekly_to_gold >> optimize_weekly_storage
optimize_weekly_storage >> end_weekly
