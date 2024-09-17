from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from breweries.tasks.landing_task import DataExtractor
from breweries.tasks.transform_task import DataTrasformer
from breweries.tasks.business_task import DataRulesInjector

default_args = {
    'start_date':datetime(2024, 9, 14),
    # 'email':['email@email.com'],
    # 'email_on_failure':True,
    # 'email_on_retry':True,
    'retries':3,
    'retry_delay':timedelta(minutes=1)}


with DAG(
    dag_id='breweries_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 9, 14),
    description='ETL from the openbrewerydb API data to analysis-ready data',
    tags=['breweries'],
    schedule='@daily',
    catchup=False
) as dag:

    start = DummyOperator(task_id='start')

    landing_task = PythonOperator(
        task_id='landing_task',
        python_callable=DataExtractor().execute)

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=DataTrasformer().execute)

    business_task = PythonOperator(
        task_id='business_task',
        python_callable=DataRulesInjector().execute)

    end = DummyOperator(task_id='end')

start >> \
    landing_task >> \
    transform_task >> \
    business_task >> \
end