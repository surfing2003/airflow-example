from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

dag = DAG(
    dag_id='example_dag_01',
    start_date=datetime(2024,10,17),
    schedule_interval='@daily'
)


start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)


bash_task = BashOperator(
    task_id='run_bash_command',
    bash_command='echo "Hello, Airflow (bash)"',
    dag = dag
)


def print_string():
    print("Hello, Airflow (python)")


python_task = PythonOperator(
    task_id='run_python_function',
    python_callable=print_string,
    dag=dag
)

class CustomOperator(BaseOperator):
    @apply_defaults
    def __init__(self, param, *args, **kwargs):
        super(CustomOperator, self).__init__(*args, **kwargs)
        self.param = param
        
    def execute(self, context):
        print(f"Custom Operator : {param}")



custom_task = CustomOperator(
    task_id='run_custom_task',
    param="Hello, Airflow (custom)",
    dag=dag
)


start >> bash_task >> python_task >> custom_task >> end