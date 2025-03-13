from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'Jesuino',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='meu_primeiro_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    tarefa = BashOperator(
        task_id='exemplo_tarefa',
        bash_command='echo "Executando o DAG no Airflow!"'
    )

