from airflow import DAG
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

def DAG_Download_data_tab_sql_from_csv_1_4(table_name):
    df = pandas.read_csv("/tmp/dm_f101_round_f.csv", delimiter=";")
    postgres_hook = PostgresHook("pw-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema="preload", if_exists="append", index=False)


default_args = {
"owner" : "ezuev", 
"start_date" : datetime(2025, 1, 12), 
"retries" : 1
}

with DAG(
	"DAG_Download_data_tab_sql_from_csv_1_4",
	default_args=default_args, 
	description="Импорт в таблицу dm _f101_round_f_v2 в схемы preload и миграция данных в ds для 1.1", 
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
        sql="sql/logstart_1_4_D.sql"
    )
    
    dm_f101_round_f_v2 = PythonOperator(
        task_id="dm_f101_round_f_v2", 
        python_callable=DAG_Download_data_tab_sql_from_csv_1_4, 
        op_kwargs={"table_name" : "dm_f101_round_f_v2"}
    )
    
    split = DummyOperator(
        task_id="split"
    )
        
    dm_f101_round_f_v2_1_4 = SQLExecuteQueryOperator( 
        task_id="dm_f101_round_f_v2_1_4", 
        conn_id="pw-db", 
        sql="sql/dm_f101_round_f_v2_1_4.sql"
        )
        
    sql_logend = SQLExecuteQueryOperator(
        task_id="sql_logend", 
        conn_id="pw-db", 
        sql="sql/logend_1_4_D.sql"
    )
    
    end = DummyOperator(
        task_id="end"
    )
    
    (
    start
    >> sql_logstart
    >> dm_f101_round_f_v2
    >> split
    >> dm_f101_round_f_v2_1_4
    >> sql_logend
    >> end
    )
    
    
