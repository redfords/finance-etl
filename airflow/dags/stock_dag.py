from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import run_data_validation

mysql_hook = MySqlHook(mysql_conn_id = 'stock_id')

def load_data():
    table_name = 'stock_market'
    file_name = 'transformed_data'

    # convert .csv into dataframe
    data = pd.read_csv('~/airflow/dags/files/' + file_name + '.csv')
    data = data.iloc[:, 1:]

    # convert dataframe into list of tuples
    rows = list(data.itertuples(index = False, name = None))

    # insert list of tuples into db
    mysql_hook.insert_rows(table = table_name, rows = rows)

# run queries and save results as txt
def save_as_txt(result, file_name):
    result.to_csv('files/' + file_name + '.txt', sep = '\t', index = False)

def validate():
	queries = run_data_validation.queries

	for query, file_name in queries.items():
		data = mysql_hook.get_pandas_df(query)
		if not data.empty:
			save_as_txt(data, file_name)

# define the default dag arguments
default_args = {
	'owner': 'joana',
	'depends_on_past': False,
	'email': ['joanapiovaroli@gmail.com'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 5,
	'retry_delay': timedelta(minutes = 1)
	}

# define the dag, start date and frequency
dag = DAG(
	dag_id = 'stock_dag',
	default_args = default_args,
	start_date = datetime(2021,5,17),
	schedule_interval = timedelta(minutes = 1440)
	)

# get the stock market data from tradingview
task1 = BashOperator(
	task_id = 'get_stock_market',
	bash_command = 'python ~/airflow/dags/get_stock_market.py' ,
	dag = dag
	)

# get stock symbol data from finnhub
task2 = BashOperator(
	task_id = 'get_stock_symbol',
	bash_command = 'python ~/airflow/dags/get_stock_symbol.py' ,
	dag = dag
	)

# get exchange rates
task3 = BashOperator(
	task_id = 'get_exchange_rate',
	bash_command = 'python ~/airflow/dags/get_exchange_rate.py' ,
	dag = dag
	)

# run etl
task4 = BashOperator(
	task_id = 'run_etl',
	bash_command = 'python ~/airflow/dags/run_etl.py' ,
	dag = dag
	)

# load transformed data into the database
task5 =  PythonOperator(
	task_id = 'load_into_db',
	provide_context = True,
	python_callable = load_data,
	dag = dag
	)

# load transformed data into the database
task6 =  PythonOperator(
	task_id = 'validate_data',
	provide_context = True,
	python_callable = validate,
	dag = dag
	)

# task hierarchy
(task1, task2, task3) >> task4 >> task5 >> task6