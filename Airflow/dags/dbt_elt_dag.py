from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='weather_dbt_elt_pipeline',
    start_date=datetime(2026, 2, 23),
    schedule='30 3 * * *',
    catchup=False,
    tags=['ELT']
) as dag:

    dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command="""
    export PATH=$PATH:/home/airflow/.local/bin &&
    cd /opt/airflow/dbt/weather_dbt &&
    dbt run --profiles-dir /home/airflow/.dbt
    """
)

    dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command="""
    export PATH=$PATH:/home/airflow/.local/bin &&
    cd /opt/airflow/dbt/weather_dbt &&
    dbt test --profiles-dir /home/airflow/.dbt
    """
)

    dbt_run >> dbt_test