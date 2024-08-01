from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonVirtualenvOperator
from pprint import pprint
from airflow.utils.task_group import TaskGroup


with DAG(
    'movie_summary' ,
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past':False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },

    max_active_tasks= 3,
    max_active_runs= 1,
    description='hello world DAG',
    # schedule_interval=timedelta(days=1),
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['api', 'movie', 'summary'],

) as dag:
    REQUIREMENTS = [
                "git+https://github.com/DONGUK777/mov_agg.git@0.5/agg",
                ]
    def gen_empty(*ids):
        tasks = []
        for id in ids:
            task = EmptyOperator(task_id=id)
            tasks.append(task)
        return tasks

    def gen_vpython(**kw):
        task = PythonVirtualenvOperator(
            task_id=kw['id'],
            python_callable=kw['fun_obj'],
            system_site_packages=False,
            requirements=REQUIREMENTS,
            op_kwargs=kw['op_kw']
            )
        return task
    
    def pro_data(**params):
        from pprint import pprint as pp
        print("@" * 33)
        print(params['task_name'])
        pp(params) # 여기는 task_name
        print("@" * 33)

    def pro_data2(task_name, **params):
        from pprint import pprint as pp
        print("@" * 33)
        print(task_name)
        pp(params) # 여기는 task_name 없을 것으로 예상
        if "task_name" in params.keys():
            print("============== 있음")
        else:
            print("============== 없음")
        print("@" * 33)

    def pro_data3(task_name):
        print("@" * 33)
        print(task_name)
        #print(params) 
        print("@" * 33)
    
    def pro_data4(task_name, ds_nodash, **kwargs):
        from pprint import pprint as pp
        print("@" * 33)
        print(task_name)
        print(ds_nodash)
        pp(kwargs) 
        print("@" * 33)
    
    start, end = gen_empty('start', 'end')
    
    apply_type = gen_vpython(
            id = "appply.type",
            fun_obj = pro_data,
            op_kw = { "task_name": "apply_type!!!" }
            )
    merge_df = gen_vpython(
            id = "merge.df",
            fun_obj = pro_data2,
            op_kw = { "task_name": "merge_df!!!" }
            )
    de_dup = gen_vpython(
            id = "de.dup",
            fun_obj = pro_data3,
            op_kw = { "task_name": "du_dup!!!" }
            )
    summary_df = gen_vpython(
            id = "summary.df",
            fun_obj = pro_data4,
            op_kw = { "task_name": "summary_df!!!" }
            )

    start >> merge_df
    merge_df >> de_dup >> apply_type
    apply_type >> summary_df >> end
