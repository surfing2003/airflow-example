from datetime import datetime

from airflow.decorators import dag, task


@dag(dag_id='example_dag_03', start_date=datetime(2024,10,16),schedule_interval='@daily')
def generate_dag():
        
    @task
    def return_string():
        return "Hello, Airflow (python)"

    @task
    def print_string(input_string):
        print(input_string)

    step_1 = return_string()
    step_2 = print_string(step_1)

dag = generate_dag() 