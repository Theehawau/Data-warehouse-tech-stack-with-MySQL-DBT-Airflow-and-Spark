from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator, task
from airflow.operators.bash import BashOperator 

from datetime import datetime

with DAG("dbt_dag", start_date = datetime(2021,9,1),
        schedule_interval="@daily", catchup = False, tags=['sensors']) as dag:
    
    go_to_dbt_folder = BashOperator(
        task_id = 'go_to_dbt_folder', 
        bash_command= 'cd /Users/user1/dbt/; source dbt-env/bin/activate;'
    )
    
    # activate_dbtenv = BashOperator(
    #     task_id = 'activate_dbtenv',
    #     bash_command='source dbt-env/bin/activate'
    # )
    
    go_to_project = BashOperator(
        task_id='go_to_project',
        bash_command= 'cd /Users/user1/dbt/; cd sensors;'
    )
    
    # check_for_errors = BashOperator(
    #     task_id = 'check_for_errors', 
    #     bash_command="cd /Users/user1/dbt/sensors; dbt debug"
    # )
    
    # load_raw_data = BashOperator(
    #     task_id = 'load_raw_data', 
    #     bash_command="dbt seed"
    # )
    
    run_transformation = BashOperator(
        task_id = 'run_transformation', 
        bash_command="cd /Users/user1/dbt/sensors; source /Users/user1/dbt/dbt-env/bin/activate; dbt run"
    )
    
    run_tests = BashOperator (
        task_id = 'run_tests',
        bash_command='cd /Users/user1/dbt/sensors; dbt test'
    )
    
    generate_docs = BashOperator(
        task_id = 'generate_docs',
        bash_command= 'cd /Users/user1/dbt/sensors; dbt docs generate'
    )
    
    deploy_docs = BashOperator(
        task_id = 'deploy_docs',
        bash_command = 'cd /Users/user1/dbt/sensors; dbt docs serve --port 8001'
    )
    
    
go_to_dbt_folder  >> go_to_project  >> run_transformation >> run_tests >> generate_docs >> deploy_docs
    