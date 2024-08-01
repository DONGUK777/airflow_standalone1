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
##    def get_data(ds_nodash):
#        from mov.api.call import save2df
#        df = save2df(ds_nodash)
#        print(df.head(5))

    def fun_multi_y(ds_nodash, url_param):
        from mov.api.call import save2df
        df = save2df(load_dt=ds_nodash, url_param=url_param)
        
        print(df[['movieCd', 'movieNm']].head(5))

        for k, v in url_param.items():
            df[k] = v

        #p_cols = list(url_param.keys()).insert(0, 'load_dt')
        p_cols = ['load_dt'] + list(url_param.keys())
        df.to_parquet('~/tmp/test_parquet',
                partition_cols=p_cols
                # partition_cols=['load_dt', 'movieKey']
        )


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
            return "get.start", "echo.task"


#    run_this = PythonOperator(
#            task_id="print_the_context",
#            python_callable=print_context,
#    )

    
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
    
    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun
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
    get_end = EmptyOperator(task_id='get.end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')
    get_start = EmptyOperator(
            task_id='get.start',
            trigger_rule="all_done"
    )
    
#    task_get = PythonVirtualenvOperator(
#            task_id='get_data',
#            python_callable=get_data,
#            requirements=["git+https://github.com/DONGUK777/mov.git@0.3/api"],
#            system_site_packages=False,
#            #venv_cache_path="/home/tommy/tmp/airflow_venv/get_data"
#        )

    with TaskGroup('processing_tasks', dag=dag) as process_group:
        multi_y = PythonVirtualenvOperator(
            task_id='multi.y',
            python_callable=fun_multi_y,
            system_site_packages=False,
            requirements=["git+https://github.com/DONGUK777/mov.git@0.3/api"],
            op_kwargs={ "url_param" : {"multiMovieYn": "Y"}}
        )
        multi_n = PythonVirtualenvOperator(
            task_id='multi_n',
            python_callable=fun_multi_y,
            system_site_packages=False,
            requirements=["git+https://github.com/DONGUK777/mov.git@0.3/api"],
            op_kwargs={ "url_param" : {"multiMovieYn": "N"}}
        ) 
        nation_k = PythonVirtualenvOperator(
            task_id='nation_k',
            python_callable=fun_multi_y,
            system_site_packages=False,
            requirements=["git+https://github.com/DONGUK777/mov.git@0.3/api"],
            op_kwargs={ "url_param" : {"repNationCd": "K"}}
        )
        nation_f = PythonVirtualenvOperator(
            task_id='nation_f',
            python_callable=fun_multi_y,
            system_site_packages=False,
            requirements=["git+https://github.com/DONGUK777/mov.git@0.3/api"],
            op_kwargs={ "url_param" : {"repNationCd": "F"}}
        )
    
    throw_err = BashOperator(
            task_id='throw.err',
            bash_command="exit 1",
            trigger_rule="all_done"
    )
    
    # task_start >> branch_op >> rm_dir 
    # rm_dir >> [task_get, multi_y, multi_n, nation_k, nation_f]
    # task_start >> task_join >> task_save
    # branch_op >> [task_get, multi_y, multi_n, nation_k, nation_f]
    # branch_op >> echo_task >> task_save
    
    # [task_get, multi_y, multi_n, nation_k, nation_f] >> task_save >> task_end
    
    task_start >> [branch_op, throw_err]
    branch_op >> [rm_dir, echo_task]
    branch_op >> get_start
    rm_dir >> get_start
    throw_err >> task_save
    get_start >> process_group
    # get_start >> [task_get, multi_y, multi_n, nation_k, nation_f]
    process_group >> get_end >> task_save >> task_end
    # [task_get, multi_y, multi_n, nation_k, nation_f] >> get_end >> task_save >> task_end
   
