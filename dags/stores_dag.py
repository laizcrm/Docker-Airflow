from airflow import DAG
from datetime import datetime as dt, timedelta
#from airflow.operators.bash import BashOperator # for quick shell tasks or running existing scripts
from airflow.operators.python import PythonOperator #for flexbility and complex logic
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.email import EmailOperator
import pandas as pd
import mysql.connector
import re
import runpy
import os

##########################################################################################
#                          DEFAULT & DAG
##########################################################################################
default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': dt(2024, 2, 5),   #the earliest date Airflow will schedule runs
    'email': ['your-email@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}


dag = DAG(
    dag_id='validate_csv',
    default_args=default_args,
    description='Validates CSV file, generates sales charts, and loads data into MySQL',
    schedule_interval='@daily',  #timedelta([hour,minutes,days])
    catchup=False,   # true= run all missed executions from @start_date -> today | false = ignore the past dates
    max_active_runs=1,
    tags=['store', 'production'], #helps to identify what DAG is for
    template_searchpath=['/opt/airflow/sql_files'],
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
    runpy.run_path('/opt/airflow/script/analysis_sales.py')


def load_csv_to_mysql():
    """Load cleaned CSV data into MySQL store_transactions table"""
    df = pd.read_csv(csv_path)

    # Clean data (same logic as analysis_sales.py)
    for col in ['MRP', 'CP', 'DISCOUNT', 'SP']:
        df[col] = df[col].replace(r'[\$,]', '', regex=True).astype(float)

    df['STORE_LOCATION'] = df['STORE_LOCATION'].map(lambda x: re.sub(r'[^\w\s]', '', x).strip())
    df['PRODUCT_ID'] = df['PRODUCT_ID'].map(lambda x: re.findall(r'\d+', x)[0] if re.findall(r'\d+', x) else x)
    df.rename(columns={'Date': 'transaction_date'}, inplace=True)

    # Connect and insert
    conn = mysql.connector.connect(
        host='airflow_db',
        database='airflow',
        user='airflow',
        password='airflow'
    )
    cursor = conn.cursor()

    # Clear old data before loading
    cursor.execute('TRUNCATE TABLE store_transactions')

    insert_sql = """INSERT INTO store_transactions
        (STORE_ID, STORE_LOCATION, PRODUCT_CATEGORY, PRODUCT_ID, MRP, CP, DISCOUNT, SP, transaction_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    rows = [tuple(row) for row in df[['STORE_ID', 'STORE_LOCATION', 'PRODUCT_CATEGORY', 'PRODUCT_ID',
                                       'MRP', 'CP', 'DISCOUNT', 'SP', 'transaction_date']].values]
    cursor.executemany(insert_sql, rows)
    conn.commit()
    print(f'✓ Loaded {len(rows)} rows into store_transactions')

    cursor.close()
    conn.close()


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

task_6 = MySqlOperator(
    task_id='create_mysql_tables',
    mysql_conn_id='mysql_conn',
    sql='create_tables.sql',
    dag=dag,
)

task_7 = PythonOperator(
    task_id='load_csv_to_mysql',
    python_callable=load_csv_to_mysql,
    dag=dag,
)

task_8 = MySqlOperator(
    task_id='insert_sales_by_category',
    mysql_conn_id='mysql_conn',
    sql='insert_sales_by_category.sql',
    dag=dag,
)

task_9 = MySqlOperator(
    task_id='insert_sales_by_store',
    mysql_conn_id='mysql_conn',
    sql='insert_sales_by_store.sql',
    dag=dag,
)

# Charts path in container
results_dir = '/opt/airflow/store_files/results'

task_10 = EmailOperator(
    task_id='send_report_email',
    to='your-email@gmail.com',
    subject='Daily Store Sales Report',
    html_content="""
        <h2>Store Sales Report</h2>
        <p>The daily sales pipeline has completed successfully.</p>
        <p>Please find the analysis charts attached.</p>
        <ul>
            <li>Sales by Category</li>
            <li>Sales by Store Location</li>
            <li>Discount Distribution</li>
            <li>Profit Margin by Category</li>
        </ul>
    """,
    files=[
        f'{results_dir}/01_sales_by_category.png',
        f'{results_dir}/02_sales_by_store.png',
        f'{results_dir}/03_discount_distribution.png',
        f'{results_dir}/04_profit_margin_by_category.png',
    ],
    dag=dag,
)

# Pipeline
task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> task_6 >> task_7 >> [task_8, task_9] >> task_10
