from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Get the path to your python script
# Assuming your folder structure is standard, we use relative paths or hardcode for local dev
PROJECT_DIR = os.getenv("AIRFLOW_HOME", "/opt/airflow") 
SCRAPER_SCRIPT = f"{PROJECT_DIR}/dags/scripts/scraper.py"

with DAG(
    'ecommerce_price_tracker',
    default_args=default_args,
    description='Scrapes Amazon prices daily and loads to Snowflake',
    schedule_interval='0 8 * * *', # Cron syntax: Runs at 08:00 AM every day
    start_date=days_ago(1),
    catchup=False,
    tags=['ecommerce', 'snowflake'],
) as dag:

    # Task 1: Run the Scraper
    # We use BashOperator because it's the cleanest way to run a separate Python script
    run_scraper = BashOperator(
        task_id='run_amazon_scraper',
        bash_command=f'python {SCRAPER_SCRIPT}',
    )

    # Task 2: (Optional Future Step) DBT Transformation
    # If you later add DBT, you would trigger it here.
    # transform_data = BashOperator(...)

    # Define dependencies (Simple line since we only have one task for now)
    run_scraper