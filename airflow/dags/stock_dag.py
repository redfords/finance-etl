from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd

def load_data():
    mysql_hook = MySqlHook(mysql_conn_id = 'stock_id')

    table_name = 'stock_market'
    file_name = 'transformed_data'

    # convert .csv into dataframe
    data = pd.read_csv('~/airflow/dags/files/' + file_name + '.csv')
    data = data.iloc[:, 1:]

    # convert dataframe into list of tuples
    rows = list(data.itertuples(index = False, name = None))

    # insert list of tuples into db
    mysql_hook.insert_rows(
		table = table_name,
		rows = rows,
		target_fields = [
			'symbol',
			'description',
			'last_gbp',
			'chg_p',
			'chg',
			'rating',
			'vol',
			'mkt_cap_gbp',
			'figi_identifier',
			'mic',
			'security_type'
		]
	)

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
	task_id = 'transform_load',
	provide_context = True,
	python_callable = load_data,
	dag = dag
	)

# task hierarchy
task1 >> task4
task2 >> task4
task3 >> task4
task4 >> task5