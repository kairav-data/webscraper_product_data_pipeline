from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 9),
    'retries': 1,
}

# Initialize the DAG
with DAG(
    'store_data_in_postgres',
    default_args=default_args,
    description='A simple DAG to store data in PostgreSQL',
    schedule_interval=None,
    catchup=False,
) as dag:
    
    # Task to create table in PostgreSQL
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS sample_data (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                value INT
            );
        """
    )

    # Function to insert data into PostgreSQL
    def insert_data():
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        pg_hook.run("""
            INSERT INTO sample_data (name, value) VALUES
            ('item1', 10),
            ('item2', 20),
            ('item3', 30);
        """)

    # Task to insert data
    insert_data_task = PythonOperator(
        task_id='insert_data_task',
        python_callable=insert_data,
    )

    # Define task dependencies
    create_table >> insert_data_task
