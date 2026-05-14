from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PYSPARK_DIR = '/opt/airflow/PySpark'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='network_intrusion_etl',
    default_args=default_args,
    description='Network Intrusion Detection ETL Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'network', 'security'],
) as dag:

    extract = BashOperator(
        task_id='extract',
        bash_command=(
            f'jupyter nbconvert --to notebook --execute '
            f'--ExecutePreprocessor.timeout=600 '
            f'--inplace {PYSPARK_DIR}/exrtract.ipynb'
        ),
    )

    transform = BashOperator(
        task_id='transform',
        bash_command=(
            f'jupyter nbconvert --to notebook --execute '
            f'--ExecutePreprocessor.timeout=600 '
            f'--inplace {PYSPARK_DIR}/transform.ipynb'
        ),
    )

    load = BashOperator(
        task_id='load',
        bash_command=(
            f'jupyter nbconvert --to notebook --execute '
            f'--ExecutePreprocessor.timeout=600 '
            f'--inplace {PYSPARK_DIR}/load.ipynb'
        ),
    )

    extract >> transform >> load
