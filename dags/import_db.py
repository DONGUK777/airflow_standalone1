from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
with DAG(
    'import_db' ,
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
  # t1, t2 and t3 are examples of tasks created by instantiating operators

    
    #task_copy = BashOperator(
     #   task_id="copy.log",
      #  bash_command="""
       #     mkdir ~/YYYYMMDD
        #    cp ~/history{YYYYMMDD}*.log ~/data/YYYYMMDD/
        #"""
    #)
    T1 = BashOperator(
        task_id = "check",
        bash_command="""
            bash {{ var.value.CHECK_SH }} ~/data/done/{{ds_nodash}}/_DONE
        """
    )

    T2 = BashOperator(task_id="to_csv",
            bash_command="""
                echo "to.csv"

                U_PATH=~/data/count/{{ds_nodash}}/count.log
                CSV_PATH=/home/tommy/data/csv/{{ds_nodash}}
                CSV_FILE=~/data/csv/{{ds_nodash}}/csv.csv
                mkdir -p $CSV_PATH
                # cat ${U_PATH} | awk '{print "{{ds}}," $2 "," $1}' > ${CSV_PATH}/csv.csv
                cat $U_PATH | awk '{print "^{{ds}}^,^" $2 "^,^" $1 "^"}' > ${CSV_PATH}
                echo $CSV_PATH
                """
    )
    #        trigger_rule="all_success"
    
    task_create_table = BashOperator(
            task_id="create.table",
            bash_command="""
                SQL={{var.value.SQL_PATH}}/create_db_table.sql
                MYSQL_PWD={{ var.value.DB_PASSWD }} mysql -u root < ${SQL}
            """
    )

    T3 = BashOperator(task_id="to_tmp",
            bash_command="""
                echo "to.tmp"
                CSV_FILE=~/data/csv/{{ds_nodash}}/csv.csv
                echo $CSV_FILE
                bash {{ var.value.SH_HOME }}/csv2mysql.sh $CSV_FILE {{ ds }}
            """
    )
    #        trigger_rule="all_success"
    

    T4 = BashOperator(task_id="to_base",
            bash_command="""
                echo "to.base"
                bash {{ var.value.SH_HOME }}/tmp2base.sh {{ ds }}

            """
    )
    #        trigger_rule="all_success"
    
    

    T5 = BashOperator(task_id="make.done",
            bash_command="""
                figlet "make.done"
                
                DONE_PATH={{ var.value.IMPORT_DONE_PATH }}/{{ds_nodash}}
                mkdir -p $DONE_PATH
                echo "IMPORT_DONE_PATH=$DONE_PATH"
                touch $DONE_PATH/_DONE

                figlet "make.done.end"
            """
    )

    task_err = BashOperator(
            task_id="err.report",
            bash_command="""
                echo "err report"
            """,
            trigger_rule="one_failed"
    )

    task_end = EmptyOperator(task_id='end',
                    trigger_rule="all_done"
    ) 

    task_start = EmptyOperator(task_id='start')
    

    task_start >> T1 >> T2 >> task_create_table >> T3 >> T4 >> T5 >> task_end
    T1 >> task_err >> task_end
