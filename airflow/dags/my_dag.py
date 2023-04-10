from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator

# Initiating the default_args
default_args = {
        'owner' : 'airflow',
        'start_date' : datetime(2022, 11, 12),
}

# Creating DAG Object
dag = DAG(dag_id='my_dag',
        default_args=default_args,
        schedule_interval='@once', 
        catchup=False
    )

# Creating tasks
start = DummyOperator(task_id = 'start', dag = dag)
end = DummyOperator(task_id = 'end', dag = dag)

 # Setting up dependencies 
start >> end 