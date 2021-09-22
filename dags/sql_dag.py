from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.contrib.sensors.file_sensor import FileSensor

from airflow.utils.dates import days_ago
import pandas as pd
import os

CUR_DIR = os.path.abspath(os.path.dirname(__file__))


def _read_csv():
    try:
        data = pd.read_csv(f'{CUR_DIR}/station_summary.csv')
        f = open (f'{CUR_DIR}/scripts/test.sql', 'a') 
        query = 'INSERT INTO stations (id, flow_99, flow_max, flow_median, flow_total, n_obs) VALUES'
        val = list(data.to_records(index=False))
        string = query + str(val).replace('[','').replace(']','')
        f.write(string)
        f.write(';')
        f.close()
    except Exception:
        print(Exception)
        
dag = DAG(
    'example_mysql',
    start_date=days_ago(2),
    template_searchpath=f'{CUR_DIR}'
)

create_table = MySqlOperator(
    task_id = 'create_table', mysql_conn_id='test1',
                sql=r""" CREATE TABLE IF NOT EXISTS stations (
                    id INT PRIMARY KEY NOT NULL,
                    flow_99 INT,
                    flow_max INT,
                    flow_median INT,
                    flow_total INT,
                    n_obs INT);""",dag=dag
)

# check_file = FileSensor(task_id='check_file', filepath='station_summary.csv')

read_csv = PythonOperator( task_id='_read_csv', python_callable=_read_csv, dag=dag)

insert_data = MySqlOperator(
    task_id = 'insert_data', mysql_conn_id='test1',
    sql='/scripts/test.sql', dag=dag
)

create_table >> read_csv >> insert_data