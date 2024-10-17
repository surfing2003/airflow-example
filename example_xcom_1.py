from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="example_xcom_1",    
    start_date=datetime(2024,10,16),
    schedule_interval=None
) as dag:

    start = DummyOperator(task_id='start')

    # python operator
    def python_operator_return_xcom():
        return "python_value"

    task_1 = PythonOperator(
        task_id='python_operator_return_xcom',
        python_callable=python_operator_return_xcom,
    )
    
    # xcom
    def push_value(**context):
        task_instance = context['task_instance']
        task_instance.xcom_push(key='xcom_push', value="xcom_value")

    def pull_value(**context):
        val = context['task_instance'].xcom_pull(key='xcom_push')
        print(val)

    task_2 = PythonOperator(
        task_id='pushed_value',
        python_callable=push_value
    )

    task_3 = PythonOperator(
        task_id='pulled_value',
        python_callable=pull_value
    )

    # jinja
    def pull_value_from_jinja(**context):
        val = context['task_instance'].xcom_pull(key='jinja_push')
        print(val)

    task_4 = BashOperator(
        task_id='jinja_template',
        bash_command='echo "{{task_instance.xcom_push(key="jinja_push", value="jinja_value")}}"'
    )

    task_5 = PythonOperator(
        task_id='pulled_value_jinja',
        python_callable=pull_value_from_jinja
    )

    start >> task_1
    start >> task_2 >> task_3
    start >> task_4 >> task_5