from random import randint
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

def random_branch_path():
    return "print_ko_1" if randint(1, 2) == 1 else "print_en_1"


dag_args = {
    "dag_id" : "example_branch_1",
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
        task_id='print_ko_1',
        bash_command='echo "안녕하세요"',
    )
    
    task_4 = BashOperator(
        task_id='print_ko_2',
        bash_command='echo "만나서 반갑습니다."',
    )

    task_5 = BashOperator(
        task_id='print_en_1',
        bash_command='echo "Hi"',
    )
    
    task_6 = BashOperator(
        task_id='print_en_2',
        bash_command='echo "Nice to meet you"',
    )

    task_7 = BashOperator(
        task_id='finish', 
        bash_command='echo "종료"',
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    task_1 >> task_2 >> task_3 >> task_4 >> task_7
    task_1 >> task_2 >> task_5 >> task_6 >> task_7
    