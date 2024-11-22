from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
import subprocess
import mysql.connector
from datetime import datetime

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'db_backup_dag_every_1_minute',
    default_args=default_args,
    description='Connect to DB, dump, and save directly to /home with timestamped filenames',
    schedule_interval='*/1 * * * *',  # Every 1 minute
    start_date=days_ago(1),
    catchup=False,
)

# Task 1: Connect to the database
def connect_to_db():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="your db password",
        )
        if connection.is_connected():
            print("Successfully connected to the MySQL database")
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        raise
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print("MySQL connection closed")

task_1 = PythonOperator(
    task_id='connect_to_db',
    python_callable=connect_to_db,
    dag=dag,
)

# Task 2: Dump the database directly to /home
def dump_db():
    db_name = "user_details"  
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')  
    dump_file_path = f"/home/db_dump_{timestamp}.sql"  # Directly save to /home
    command = f"mysqldump -u root -p root {db_name} > {dump_file_path}"
    subprocess.run(command, shell=True, check=True)
    print(f"Database dumped directly to {dump_file_path}")
    return dump_file_path  

task_2 = PythonOperator(
    task_id='dump_db',
    python_callable=dump_db,
    dag=dag,
)

# task dependencies
task_1 >> task_2
