from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook 
from airflow.operators.python_operator import PythonOperator

import pandas
from datetime import datetime


def insert_data_tab4(table_name):
    df = pandas.read_csv(f"/files/{table_name}.csv", delimiter=";")
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema="dsl", if_exists="append", index=False)


default_args = {
"owner" : "ezuev", 
"start_date" : datetime(2025, 1, 2), 
"retries" : 1
}

with DAG(
	"insert_data_tab4",
	default_args=default_args, 
	description="Загрyзка данных в dsl", 
	catchup=False, 
	schedule="0 0 * * *"
) as dag: 

    start = DummyOperator(
        task_id="start"
    )    


    md_account_d = PythonOperator(
        task_id="md_account_d", 
        python_callable=insert_data_tab4, 
        op_kwargs={"table_name" : "md_account_d"}
    )    
    
    md_currency_d = PythonOperator(
        task_id="md_currency_d", 
        python_callable=insert_data_tab4, 
        op_kwargs={"table_name" : "md_currency_d"}
    )
    
    md_exchange_rate_d = PythonOperator(
        task_id="md_exchange_rate_d", 
        python_callable=insert_data_tab4, 
        op_kwargs={"table_name" : "md_exchange_rate_d"}
    )    
    
    md_ledger_account_s = PythonOperator(
        task_id="md_ledger_account_s", 
        python_callable=insert_data_tab4, 
        op_kwargs={"table_name" : "md_ledger_account_s"}
    )
    
    end = DummyOperator(
        task_id="end"
    )    

    (
    start
    >> [md_account_d, md_currency_d, md_exchange_rate_d, md_ledger_account_s] 
    >> end
    )
