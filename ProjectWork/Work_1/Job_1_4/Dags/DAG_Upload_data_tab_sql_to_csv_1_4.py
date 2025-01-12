from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.configuration import conf
from airflow.models import Variable

import pandas
import time
from datetime import datetime

PATH = Variable.get("pw1_path")
conf.set("core", "template_searchpath", PATH)

def DAG_Upload_data_tab_sql_to_csv_1_4(table_name):
    postgres_hook = PostgresHook("pw-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df = pandas.read_sql(f""" SELECT * FROM dm.{table_name}; """, con=engine)
    df.to_csv(f'/tmp/{table_name}.csv', sep=';', index=False)

default_args = {
"owner" : "ezuev", 
"start_date" : datetime(2025, 1, 11), 
"retries" : 1
}

with DAG(
	"DAG_Upload_data_tab_sql_to_csv_1_4",
	default_args=default_args, 
	description="Выгрузка данных из таблицы в CSV файл для 1.1",
	catchup=False,
    template_searchpath=[PATH],
	schedule="0 0 * * *"
) as dag: 
    
    start = DummyOperator(
        task_id="start"
    )
    
    sql_logstart = SQLExecuteQueryOperator(
        task_id="sql_logstart", 
        conn_id="pw-db", 
        sql="sql/logstart_1_4.sql"
    )
    
    dm_f101_round_f = PythonOperator(
        task_id="dm_f101_round_f", 
        python_callable=DAG_Upload_data_tab_sql_to_csv_1_4, 
        op_kwargs={"table_name" : "dm_f101_round_f"}
    )
    
    sql_logend = SQLExecuteQueryOperator(
        task_id="sql_logend", 
        conn_id="pw-db", 
        sql="sql/logend_1_4.sql"
    )
    
    end = DummyOperator(
        task_id="end"
    )
    
    (
    start
    >> sql_logstart
    >> dm_f101_round_f
    >> sql_logend
    >> end
    )
    
    
