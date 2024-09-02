from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
with DAG(
    'fish_test' ,
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past':False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='hello world DAG',
    # schedule_interval=timedelta(days=1),
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['simple', 'bash', 'etl' 'shop' 'db' 'history'],
) as dag:

    load_csv = EmptyOperator(
        task_id = "load.csv"
    )

    predict = EmptyOperator(
            task_id="predict",
    )

    agg = EmptyOperator(
            task_id="agg",
    )


    end = EmptyOperator(task_id='end',
                    trigger_rule="all_done"
    )

    start = EmptyOperator(task_id='start')

    start >> load_csv >> predict >> agg >> end
