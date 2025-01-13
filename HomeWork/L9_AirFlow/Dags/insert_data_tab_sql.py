from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.configuration import conf
from airflow.models import Variable

import pandas
from datetime import datetime

PATH = Variable.get("my_path")
conf.set("core", "template_searchpath", PATH)

def insert_data_tab_sql(table_name):
    df = pandas.read_csv(PATH + f"{table_name}.csv", delimiter=";")
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema="stage", if_exists="append", index=False)


default_args = {
"owner" : "ezuev", 
"start_date" : datetime(2025, 1, 2), 
"retries" : 1
}

with DAG(
    "insert_data_tab_sql",
    default_args=default_args, 
    description="Загрyзка данных в stage", 
    catchup=False,
    template_searchpath="/files/",
    schedule="0 0 * * *"
) as dag: 

    start = DummyOperator(
        task_id="start"
    )    

    ft_balance_f = PythonOperator(
        task_id="ft_balance_f", 
        python_callable=insert_data_tab_sql, 
        op_kwargs={"table_name" : "ft_balance_f"}
    )    
    
    ft_posting_f = PythonOperator(
        task_id="ft_posting_f", 
        python_callable=insert_data_tab_sql, 
        op_kwargs={"table_name" : "ft_posting_f"}
    )
    
    split = DummyOperator(
        task_id="split"
    )
    
    sql_ft_posting_f = SQLExecuteQueryOperator( 
        task_id="sql_ft_posting_f", 
        conn_id="postgres-db", 
        sql="sql/ft_posting_f.sql"
        )
        
    sql_ft_balance_f = SQLExecuteQueryOperator(
        task_id="sql_ft_balance_f", 
        conn_id="postgres-db", 
        sql="sql/ft_balance_f.sql"
    )
    
    call_get_posting_data_by_date = SQLExecuteQueryOperator( 
        task_id="call_get_posting_data_by_date",
        conn_id="postgres-db",
        sql="CALL dm.get_posting_data_by_date()"
    )
 
    end = DummyOperator(
        task_id="end"
    )    

    (
    start
    >> [ft_balance_f, ft_posting_f] 
    >> split
    >> [sql_ft_balance_f, sql_ft_posting_f] 
    >> call_get_posting_data_by_date
    >> end
    )
    
    
