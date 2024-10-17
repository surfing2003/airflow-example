from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def calculate_add(x,y, **context) :
    result = x + y
    context['task_instance'].xcom_push(key='calculate_result', value = result)

def calculate_mul(y, **context) :
    result = context["task_instance"].xcom_pull(key='calculate_result') * y
    context['task_instance'].xcom_push(key='calculate_result', value = result)
    
def print_result(**context) :
    result = context["task_instance"].xcom_pull(key='calculate_result')
    # result = context["task_instance"].xcom_pull(task_ids='calculate_add', key='calculate_result')
    # result = context["task_instance"].xcom_pull(task_ids=['calculate_add','calculate_mul'], key='calculate_result')
    print("result : ", result)


with DAG(
    dag_id="example_xcom_2",    
    start_date=datetime(2024,10,16),
    schedule_interval=None
) as dag:

    task_1 = PythonOperator(
        task_id = 'calculate_add',
        python_callable = calculate_add, 
        op_kwargs = {'x' : 10, 'y': 5}
    )

    task_2 = PythonOperator(
        task_id = 'calculate_mul',
        python_callable = calculate_mul,
        op_kwargs = {'y': 4}
    )

    task_3 = PythonOperator(
        task_id = 'print_result', 
        python_callable = print_result
    )

    task_1 >> task_2 >> task_3