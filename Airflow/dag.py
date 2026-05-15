from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Directory containing PySpark notebooks
PYSPARK_DIR = '/opt/airflow/PySpark'


def notebook_command(notebook_name):
    """
    Generate bash command to execute a Jupyter notebook.

    Args:
        notebook_name (str): Name of the notebook file.

    Returns:
        str: Executable bash command.
    """
    return (
        f'echo "Starting {notebook_name} execution..." && '
        f'jupyter nbconvert --to notebook --execute '
        f'--ExecutePreprocessor.timeout=600 '
        f'--inplace {PYSPARK_DIR}/{notebook_name} && '
        f'echo "{notebook_name} executed successfully."'
    )


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

    # Extract task
    extract = BashOperator(
        task_id='extract',
        bash_command=notebook_command('exrtract.ipynb'),
    )

    # Transform task
    transform = BashOperator(
        task_id='transform',
        bash_command=notebook_command('transform.ipynb'),
    )

    # Load task
    load = BashOperator(
        task_id='load',
        bash_command=notebook_command('load.ipynb'),
    )

    # Task pipeline order
    extract >> transform >> load