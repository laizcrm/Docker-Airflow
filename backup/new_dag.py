
#---------------------------------------------------------------
               #EXERCISE 1
#Create a new directory (let say 'test_dir') 
# inside the dags folder & crosscheck the results at task level.

# DAG = definição
# Task = execução

#---------------------------------------------------------------

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime as dt
import os


#TASK - funcao do operador
def create_dir():
    dags_path = '/opt/airflow/dags'
    path = os.path.join(dags_path, "test_dir")
    os.makedirs(path, exist_ok=True)
    print(f"Diretório criado em: {path}")



#DAG
dag = DAG(
    dag_id="new_folder",
    start_date=dt(2024, 1, 21),
    schedule_interval=None,
    catchup=False,
)

#define quando e como a funcao vai ser operada
create_dir_task = PythonOperator(
    task_id="create_new_folder",
    python_callable=create_dir,
    dag=dag,
)



