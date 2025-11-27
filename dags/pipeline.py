from airflow import DAG
from airflow.operators.python import task
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from airflow.utils.dates import days_ago


# Define DAG

with DAG(
    dag_id = 'nasa_apod_postgres',
    start_date=days_ago(1),
    schedule='@daily',
    catchup=False
    
) as dag:
    
    # step 1 Create table if it doent exists
    @task
    def create_table():
        ## initialize the Postgreshook
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")

        ## SQL query to create the table
        create_table_query="""
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        postgres_hook.run(create_table_query)
    # step 2 extract data from NASA APOD API[Extract pipeline]
        

        extract_apod = SimpleHttpOperator(
            task_id='extract_apod',
            http_conn_id='nasa_apod_api', #connecrtion id defined in airflow for nasa api
            end_point = planetary/apod, #nasa api endpoint for airflow
            method='GET',
            data = {"api_key":"{{ nasa_api_key.extra_djson.api_key }}"}, # use api kwy from the connetion
            response_filter=lambda response: json.loads(response.text),
        ) 
    # step 3 Transform the data (pic the info i need to save)
        
    @task
    def transform_data(response):
        apod_data = {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data
    
    # step 4 Load the data into Postgres [Load pipeline]
    @task
    def load_data(apod_data):
        # initialize the Postgreshook
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")
        
        # Define the inser querry
        insert_query="""
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);    
        
        """
        
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))
   
    # step 5 verify with DB viewer
    
    # step 6 define task dependencies
     ## Extract
    create_table() >> extract_apod  ## Ensure the table is create befor extraction
    api_response=extract_apod.output
    ## Transform
    transformed_data=transform_apod_data(api_response)
    ## Load
    load_data_to_postgres(transformed_data)

