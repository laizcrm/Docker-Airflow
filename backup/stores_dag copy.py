from airflow import DAG
from datetime import datetime as dt, timedelta
from airflow.operators.bash import BashOperator

###########################################################################
#                           Default arguments

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': dt(2024, 2, 5), #
    'email': ['your-email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

###########################################################################
#                                 DAG

dag = DAG(
    dag_id='validate_csv',
    default_args=default_args,
    description='Validates CSV file and generates sales charts',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['store', 'production'],
)

###########################################################################
#                               TASKS

# CSV path in container
csv_path = "/opt/airflow/store_files/raw_store_transactions.csv"

# Task 1: Check if file exists
task_1 = BashOperator(
    task_id='check_file_exists',
    bash_command=f'''
    FILE="{csv_path}"

    if [ ! -f "$FILE" ]; then
        echo "✗ File does not exist: $FILE"
        exit 1
    fi

    echo "✓ File exists: $FILE"
    ''',
    dag=dag,
)

# Task 2: Check if file is not empty
task_2 = BashOperator(
    task_id='check_not_empty',
    bash_command=f'''
    FILE="{csv_path}"

    if [ ! -s "$FILE" ]; then
        echo "✗ File is empty!"
        exit 1
    fi

    echo "✓ File is not empty"
    ''',
    dag=dag,
)

# Task 3: Check minimum number of lines
task_3 = BashOperator(
    task_id='check_min_lines',
    bash_command=f'''
    FILE="{csv_path}"
    MIN_LINES=2

    LINES=$(wc -l < "$FILE")

    if [ "$LINES" -lt "$MIN_LINES" ]; then
        echo "✗ File has only $LINES lines (minimum: $MIN_LINES)!"
        exit 1
    fi

    echo "✓ File has $LINES lines"
    ''',
    dag=dag,
)

# Task 4: Show columns and date from CSV
task_4 = BashOperator(
    task_id='show_columns',
    bash_command=f'''
    FILE="{csv_path}"

    HEADER=$(head -n 1 "$FILE")
    DATE=$(head -n 2 "$FILE" | tail -1 | cut -d',' -f9)

    echo "Columns: $HEADER"
    echo "Date: $DATE"
    ''',
    dag=dag,
)

# Task 5: Generate analysis charts
task_5 = BashOperator(
    task_id='generate_charts',
    bash_command='python /opt/airflow/script/analise_vendas.py',
    dag=dag,
)

# Pipeline
task_1 >> task_2 >> task_3 >> task_4 >> task_5
