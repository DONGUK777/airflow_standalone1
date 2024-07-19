from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator
with DAG(
    'helloworld',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['helloworld'],
) as dag:
 #t1, t2 and t3 are examples of tasks created by instantiating operators
    task_start = DummyOperator(task_id='start')
    July_impression_task = EmptyOperator(task_id='July_impression')
    July_bounce_task = EmptyOperator(task_id='July_bounce')
    July_click_task = EmptyOperator(task_id='July_click')
    July_conversion_task = EmptyOperator(task_id='July_conversion')
    July_CPM_task = EmptyOperator(task_id='July_CPM')
    July_sales_task = EmptyOperator(task_id='sales_ROAS')
    July_value_task = EmptyOperator(task_id='July_value')
    August_impression_task = EmptyOperator(task_id='August_impression')
    August_bounce_task = EmptyOperator(task_id='August_bounce')
    August_click_task = EmptyOperator(task_id='August_click')
    August_conversion_task = EmptyOperator(task_id='August_conversion')
    August_CPM_task = EmptyOperator(task_id='August_CPM')
    August_sales_task = EmptyOperator(task_id='August_sales')
    August_value_task = EmptyOperator(task_id='August_value')
    task_end = DummyOperator(task_id='end')

    task_start >> [July_impression_task,  August_impression_task]
    July_impression_task >> [July_bounce_task, July_click_task]
    July_bounce_task >> July_CPM_task
    July_click_task >> July_conversion_task >> July_sales_task
    [July_CPM_task, July_sales_task] >> July_value_task
    August_impression_task >> [August_bounce_task, August_click_task]
    August_bounce_task >> August_CPM_task
    August_click_task >> August_conversion_task >> August_sales_task
    [August_CPM_task, August_sales_task] >> August_value_task
    [July_value_task, August_value_task] >> task_end

    
    
    
    
    
    
