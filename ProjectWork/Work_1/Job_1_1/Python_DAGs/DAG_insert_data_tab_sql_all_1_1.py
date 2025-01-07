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

def DAG_insert_data_tab_sql_all_1_1(table_name):
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
	"DAG_insert_data_tab_sql_all_1_1",
	default_args=default_args, 
	description="Импорт таблиц в схему preload и миграция данных в ds для 1.1", 
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
        sql="sql/logstart_1_1.sql"
    )
    
    delay_for_logs = PythonOperator(
        task_id="delay_python_task",
         python_callable=lambda: time.sleep(5)
    )
    
    ft_balance_f = PythonOperator(
        task_id="ft_balance_f", 
        python_callable=DAG_insert_data_tab_sql_all_1_1, 
        op_kwargs={"table_name" : "ft_balance_f"}
    )
        
    ft_posting_f = PythonOperator(
        task_id="ft_posting_f", 
        python_callable=DAG_insert_data_tab_sql_all_1_1, 
        op_kwargs={"table_name" : "ft_posting_f"}
    )
    
    md_account_d = PythonOperator(
        task_id="md_account_d", 
        python_callable=DAG_insert_data_tab_sql_all_1_1, 
        op_kwargs={"table_name" : "md_account_d"}
    )    
    
    md_currency_d = PythonOperator(
        task_id="md_currency_d", 
        python_callable=DAG_insert_data_tab_sql_all_1_1, 
        op_kwargs={"table_name" : "md_currency_d"}
    )
    
    md_exchange_rate_d = PythonOperator(
        task_id="md_exchange_rate_d", 
        python_callable=DAG_insert_data_tab_sql_all_1_1, 
        op_kwargs={"table_name" : "md_exchange_rate_d"}
    )    
    
    md_ledger_account_s = PythonOperator(
        task_id="md_ledger_account_s", 
        python_callable=DAG_insert_data_tab_sql_all_1_1, 
        op_kwargs={"table_name" : "md_ledger_account_s"}
    )
    
    split = DummyOperator(
        task_id="split"
    )
        
    sql_ft_posting_f = SQLExecuteQueryOperator( 
        task_id="sql_ft_posting_f", 
        conn_id="pw-db", 
        sql="sql/ft_posting_f_1_1.sql"
        )
        
    sql_ft_balance_f = SQLExecuteQueryOperator(
        task_id="sql_ft_balance_f", 
        conn_id="pw-db", 
        sql="sql/ft_balance_f_1_1.sql"
    )
    
    sql_md_account_d = SQLExecuteQueryOperator( 
        task_id="sql_md_account_d", 
        conn_id="pw-db", 
        sql="sql/md_account_d_1_1.sql"
        )
        
    sql_md_currency_d = SQLExecuteQueryOperator(
        task_id="sql_md_currency_d", 
        conn_id="pw-db", 
        sql="sql/md_currency_d_1_1.sql"
    )
    
    sql_md_exchange_rate_d = SQLExecuteQueryOperator( 
        task_id="sql_md_exchange_rate_d", 
        conn_id="pw-db", 
        sql="sql/md_exchange_rate_d_1_1.sql"
        )
        
    sql_md_ledger_account_s = SQLExecuteQueryOperator(
        task_id="sql_md_ledger_account_s", 
        conn_id="pw-db", 
        sql="sql/md_ledger_account_s_1_1.sql"
    )
    
    sql_logend = SQLExecuteQueryOperator(
        task_id="sql_logend", 
        conn_id="pw-db", 
        sql="sql/logend_1_1.sql"
    )
    
    end = DummyOperator(
        task_id="end"
    )
    
    (
    start
    >> sql_logstart
    >> delay_for_logs
    >> [ft_balance_f, ft_posting_f, md_account_d, md_currency_d, md_exchange_rate_d, md_ledger_account_s] 
    >> split
    >> [sql_ft_balance_f, sql_ft_posting_f, sql_md_account_d, sql_md_currency_d, sql_md_exchange_rate_d, sql_md_ledger_account_s] 
    >> sql_logend
    >> end
    )
    
    
