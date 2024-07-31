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


with DAG(
    'movie' ,
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
    tags=['api', 'movie', 'ant'],
) as dag:
  # t1, t2 and t3 are examples of tasks created by instantiating operators
    def get_data(ds_nodash):
        from mov.api.call import save2df
        df = save2df(ds_nodash)
        print(df.head(5))
    
    def save_data(ds_nodash):
        from mov.api.call import apply_type2df
        df = apply_type2df(load_dt=ds_nodash)
        print("*" * 33)
        print(df.head(10))
        print("*" * 33)
        print(df.dtypes)
        
        # 개봉일 기준 그룹핑 누적 관객수 합
        print("개봉일 기준 그룹핑 누적 관객수 합")
        g = df.groupby('openDt')
        sum_df = g.agg({'audiCnt': 'sum'}).reset_index()
        print(sum_df)


#    def print_context(ds=None, **kwargs):
#        """Print the Airflow context and ds variable from the context."""
#        print("::group::All kwargs")
#        pprint(kwargs)
#        print(kwargs)
#        print("::endgroup::")
#        print("::group::Context variable ds")
#        print(ds)
#        print("::endgroup::")
#        return "Whatever you return gets printed in the logs"

    def branch_fun(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        path = os.path.join(home_dir, f"tmp/test_parquet/load_dt={ds_nodash}")
        if os.path.exists(path):
            return "rm.dir"
        else:
            return "get_data", "echo.task"

    branch_op = BranchPythonOperator(
            task_id="branch.op",
            python_callable=branch_fun
    )

#    run_this = PythonOperator(
#            task_id="print_the_context",
#            python_callable=print_context,
#    )

    task_get = PythonVirtualenvOperator(
        task_id='get_data',
        python_callable=get_data,
        requirements=["git+https://github.com/DONGUK777/mov.git@0.3/api"],
        system_site_packages=False,
        trigger_rule="all_done",
        #venv_cache_path="/home/tommy/tmp/airflow_venv/get_data"
    )
    
    #task_copy = BashOperator(
     #   task_id="copy.log",
      #  bash_command="""
       #     mkdir ~/YYYYMMDD
        #    cp ~/history{YYYYMMDD}*.log ~/data/YYYYMMDD/
        #"""
    #)
    task_save = PythonVirtualenvOperator(
        task_id = "save_data",
        python_callable=save_data,
        system_site_packages=False,
        trigger_rule="one_success",
        requirements=["git+https://github.com/DONGUK777/mov.git@0.3/api"],
        #venv_cache_path="/home/tommy/tmp/airflow_venv/get_data"
    )
    
    rm_dir = BashOperator(
            task_id='rm.dir',
            bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}',
            trigger_rule="all_success"   
    )

    echo_task = BashOperator(
        task_id='echo.task',
        bash_command="echo 'task'"
    )


#    task_err = BashOperator(
#        task_id="err.report",
#        bash_command="""
#        """,
#        trigger_rule="one_failed"
#    )


    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')
    
    multi_y = EmptyOperator(task_id='multi_y') #다양성 영화 유무
    multi_n = EmptyOperator(task_id='multi_n') 
    nation_k = EmptyOperator(task_id='nation_k') #한국/외국 영화
    nation_f = EmptyOperator(task_id='nation_f')
    
    task_join = BashOperator(
            task_id='join',
            bash_command="exit 1",
            trigger_rule="all_done"
    )
    
    task_start >> branch_op >> rm_dir 
    rm_dir >> [task_get, multi_y, multi_n, nation_k, nation_f]
    task_start >> task_join >> task_save
    
    branch_op >> [task_get, multi_y, multi_n, nation_k, nation_f]
    branch_op >> echo_task >> task_save
    
    [task_get, multi_y, multi_n, nation_k, nation_f] >> task_save >> task_end
    
    
