from airflow import DAG
from datetime import datetime as dt, timedelta
#from airflow.operators.bash import BashOperator # for quick shell tasks or running existing scripts
from airflow.operators.python import PythonOperator #for flexbility and complex logic 
import pandas as pd
import os

##########################################################################################
#                          DEFAULT & DAG
##########################################################################################
default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': dt(2024, 2, 5),   #the earliest date Airflow will schedule runs
    'email': ['your-email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}


dag = DAG(
    dag_id='validate_csv',
    default_args=default_args,
    description='Validates CSV file and generates sales charts',
    schedule_interval='@daily',  #timedelta([hour,minutes,days])
    catchup=False,   # true= run all missed executions from @start_date -> today | false = ignore the past dates
    max_active_runs=1,
    tags=['store', 'production'], #helps to identify what DAG is for
)


##########################################################################################
#                               FUNCTIONS
##########################################################################################

# CSV path in container
csv_path = "/opt/airflow/store_files/raw_store_transactions.csv"


def check_file_exists():
    """Check if CSV file exists"""
    if not os.path.isfile(csv_path):
        raise FileExistsError(f'file doees not exist:{csv_path}') #interrupt execution immediately & mark as error
    
def check_file_not_empty():
    """Check if CSV file is not empty"""
    if os.path.getsize(csv_path) == 0:
        raise ValueError(f'File is empty:{csv_path}')
    
def check_min_lines():
    """Check if the file has min required lines"""
    min_lines = 2 
    with open(csv_path, 'r') as file:
        row_count = len(file.readlines())
    if row_count < min_lines:
        raise ValueError(f'File has only {row_count} lines (minimum: {min_lines})')
    print(f'✓ Number of lines: {row_count}')

def show_columns_and_date():
    """Show CSV header columns and date from first row"""
    with open(csv_path, 'r') as file:
        header = file.readline().strip()      #header
        first_row = file.readline().strip()  # first data
    columns = first_row.split(',')
    date = columns[8] if len(columns) > 8 else 'N/A' #colunm[8] = data 
    print(f'Columns: {header}')
    print(f'Date: {date}')    


def generate_graphs():
    """Run analysis script"""
    exec(open('/opt/airflow/script/analise_vendas.py').read())


##########################################################################################
#                               OPERATORS & PIPELINE
##########################################################################################


task_1 = PythonOperator(
    task_id='check_file_exists',
    python_callable=check_file_exists,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='check_file_not_empty',
    python_callable=check_file_not_empty,
    dag=dag,
)

task_3 = PythonOperator(
    task_id='check_min_lines',
    python_callable=check_min_lines,
    dag=dag,
)

task_4 = PythonOperator(
    task_id='show_columns_and_date',
    python_callable=show_columns_and_date,
    dag=dag,
)

task_5 = PythonOperator(
    task_id='generate_charts',
    python_callable=generate_graphs,
    dag=dag,
)

# Pipeline
task_1 >> task_2 >> task_3 >> task_4 >> task_5
