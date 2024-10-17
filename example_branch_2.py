from random import randint
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

def random_branch_path():
    now = randint(1,3)
    if now == 1:
        return "print_a"
    elif now == 2:
        return "print_b"
    else:
        return "print_c"


dag_args = {
    "dag_id" : "example_branch_2",
    "start_date": datetime(2024,10,16),
    "schedule_interval" : None
}

with DAG(**dag_args) as dag:

    task_1 = BashOperator(
        task_id='print_date', 
        bash_command='date',
    )

    task_2 = BranchPythonOperator(
        task_id='branch', 
        python_callable=random_branch_path,
    )

    task_3 = BashOperator(
        task_id='print_a',
        bash_command='echo "AAAA"',
    )
    
    task_4 = BashOperator(
        task_id='print_b',
        bash_command='echo "BBBB"',
    )

    task_5 = BashOperator(
        task_id='print_c',
        bash_command='echo "CCCC"',
    )
    
    task_7 = BashOperator(
        task_id='finish', 
        bash_command='echo "종료"',
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    task_1 >> task_2 >> task_3 >> task_7
    task_1 >> task_2 >> task_4 >> task_7
    task_1 >> task_2 >> task_5 >> task_7