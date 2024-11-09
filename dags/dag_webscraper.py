

from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

#1) fetch product data (extract) 2) clean data (transform)


def scraper_data(num_product, ti):

    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'}

    # Base URL of the Amazon search results for data science products
    base_url = f"https://webscraper.io/test-sites/e-commerce/static/computers/tablets?"

    products = []
    seen_titles = set()  # To keep track of seen titles

    page = 1

    while len(products) < num_product:
        url = f"{base_url}page={page}"
        
        # Send a request to the URL
        response = requests.get(url, headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the content of the request with BeautifulSoup
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Find product containers (you may need to adjust the class names based on the actual HTML structure)
            product_containers = soup.find_all('div', class_='col-md-4 col-xl-4 col-lg-4')
            
            # Loop through the product containers and extract data
            for product in product_containers:
                title = product.find('a', class_='title')
                price = product.find('h4', class_='price float-end card-title pull-right')
                review = product.find('p', class_='review-count float-end')
                
                if title and price and review:
                    product_title = title.text.strip()
                    
                    # Check if title has been seen before
                    if product_title not in seen_titles:
                        seen_titles.add(product_title)
                        products.append({
                            "Title": product_title,
                            "Price": price.text.strip(),
                            "review": review.text.strip(),
                        })
            
            # Increment the page number for the next iteration
            page += 1
        else:
            print("Failed to retrieve the page")
            break

    # Limit to the requested number of products
    products = products[:num_product]
    
    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(products)
    
    # Remove duplicates based on 'Title' column
    df.drop_duplicates(subset="Title", inplace=True)
    
    # Push the DataFrame to XCom
    ti.xcom_push(key='product_data', value=df.to_dict('records'))

#3) create and store data in table on postgres (load)
    
def insert_product_data_into_postgres(ti):
    product_data = ti.xcom_pull(key='product_data', task_ids='fetch_product_data')
    if not product_data:
        raise ValueError("No product data found")

    postgres_hook = PostgresHook(postgres_conn_id='products_Connection')
    insert_query = """
    INSERT INTO products (title, price, review)
    VALUES (%s, %s, %s)
    """
    for product in product_data:
        postgres_hook.run(insert_query, parameters=(product['Title'], product['Price'], product['review']))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_amazon_products',
    default_args=default_args,
    description='A simple DAG to fetch product data from Amazon and store it in Postgres',
    schedule_interval=timedelta(days=1),
)

#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres


fetch_product_data_task = PythonOperator(
    task_id='fetch_product_data',
    python_callable=scraper_data,
    op_args=[10],  # Number of products to fetch
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='products_Connection',
    sql="""
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        price TEXT,
        review TEXT
    );
    """,
    dag=dag,
)

insert_product_data_task = PythonOperator(
    task_id='insert_product_data',
    python_callable=insert_product_data_into_postgres,
    dag=dag,
)

#dependencies

fetch_product_data_task >> create_table_task >> insert_product_data_task
