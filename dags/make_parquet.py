from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd

def gen_emp(id, rule="all_success"):
    op  = EmptyOperator(task_id=id, trigger_rule=rule)
    return op


with DAG(
    'make_parquet',
    default_args={
        'depends_on_past':False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='Making Parquet DAG',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['simple','bash','etl','shop','db','history'],
) as dag:
     task_check = BashOperator(
        task_id="check",
        bash_command="""
            echo "check"
            DONE_FILE={{ var.value.IMPORT_DONE_PATH }}/{{ds_nodash}}/_DONE
            bash {{ var.value.CHECK_SH }} $DONE_FILE
        """
     )
     task_parquet = BashOperator(
        task_id="parquet",
        bash_command="""
            echo "parquet"
            READ_PATH="~/data/csv/{{ds_nodash}}/csv.csv"
            SAVE_PATH="~/tmp/partition_parquet"
            python ~/airflow/py/csv2parquet.py $READ_PATH $SAVE_PATH
        """
     )
     task_done = BashOperator(
        task_id="make_done",
        bash_command="""
            echo "done"
            DONE_PATH=~/data/parquet_done/{{ds_nodash}}
            echo "$DONE_PATH"
            mkdir -p ${DONE_PATH}
            touch ${DONE_PATH}/_DONE
        """
     )

     task_err = BashOperator(
        task_id="err.report",
        bash_command="""
            echo "err report"
        """,
        trigger_rule="one_failed"
     )

     task_end = gen_emp('end','all_done')
     task_start = gen_emp('start')

     task_start >> task_check >> task_parquet
     task_check >> task_err >>  task_end
     task_parquet >> task_done >> task_end
