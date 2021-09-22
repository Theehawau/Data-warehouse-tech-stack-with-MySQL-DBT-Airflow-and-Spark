from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator 
from airflow.providers.mysql.operators.mysql import MySqlOperator

from random import randint
from datetime import datetime

def _training_model():
    return randint(1,10)

def _choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids = [
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])
    best_accuracy = max(accuracies)
    if (best_accuracy > 8) :
        return 'accurate'
    return 'inaccurate'    

with DAG("my_dag", start_date = datetime(2021,1,1),
        schedule_interval="@daily", catchup = False) as dag:
    
    training_model_A = PythonOperator(
        task_id = "training_model_A",
        python_callable = _training_model
    )
    
    training_model_B = PythonOperator(
        task_id = "training_model_B",
        python_callable = _training_model
    )
    
    training_model_C = PythonOperator(
        task_id = "training_model_C",
        python_callable = _training_model
    )
    
    choose_best_model = BranchPythonOperator(
        task_id = "choose_best_model",
        python_callable = _choose_best_model
    )
    
    accurate = BashOperator(
        task_id = "accurate",
        bash_command = "echo 'accurate' "
    )
    
    inaccurate = BashOperator(
        task_id = "inaccurate",
        bash_command = "echo 'inaccurate' "
    )
    
    drop_table_mysql_task = MySqlOperator(
        task_id='create_table_mysql', mysql_conn_id='test1', 
                    sql=r"""INSERT TABLE accuracy(
                        col VARCHAR(50) ); 
                        INSERT INTO accuracy VALUES ('testing')"""
    )
    
[training_model_A,training_model_B,training_model_C] >> choose_best_model >> [accurate,inaccurate] >> drop_table_mysql_task