from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.configuration import conf
from airflow.models import Variable

import pandas
from datetime import datetime

PATH = Variable.get("pw1_path")
conf.set("core", "template_searchpath", PATH)

def Create_Schema_Table_1_1(table_name):
    df = pandas.read_csv(PATH + f"{table_name}.csv", delimiter=";")
    postgres_hook = PostgresHook("pw-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema="preload", if_exists="append", index=False)


default_args = {
"owner" : "ezuev", 
"start_date" : datetime(2025, 1, 6), 
"retries" : 1
}

with DAG(
	"Create_Schema_Table_1_1",
	default_args=default_args, 
	description="Создание схем и таблиц для 1.1", 
	catchup=False,
    template_searchpath=[PATH],
	schedule="0 0 * * *"
) as dag: 
    
    start = DummyOperator(
        task_id="start"
    )
    
    Create_Schema_Table_1_1 = SQLExecuteQueryOperator(
        task_id="1_1_Create_Schema_Table", 
        conn_id="pw-db", 
        sql="sql/1_1_Create_Schema_Table.sql"
    )
    
    end = DummyOperator(
        task_id="end"
    )
    
    (
    start
    >> Create_Schema_Table_1_1
    >> end
    )   
    
