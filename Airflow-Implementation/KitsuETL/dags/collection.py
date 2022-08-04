from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta


from utils.push_data import ETL
api = ETL()

default_args ={
    'owner':'Airflow',
    'start_date':datetime(2022,6,9),
    'retries':1,
    'retry_delay':timedelta(seconds=5)
}
dag = DAG('anime_extractions',default_args=default_args, schedule_interval='@weekly',catchup=False)


t3 = BashOperator(task_id='installation_of_nodejs', bash_command='apt-get install nodejs -y && apt-get install npm -y',
                    retry_delay=timedelta(seconds=15),dag=dag)

t1 = BashOperator(task_id='extraction_of_data', bash_command='node kitsu_scrapper.js Anime anime_sources.json', retries=2,
                    retry_delay=timedelta(seconds=15),dag=dag)

t2 = BashOperator(task_id='check_of_data', bash_command='shasum .anime_sources.json', retries=2,
                    retry_delay=timedelta(seconds=15),dag=dag)