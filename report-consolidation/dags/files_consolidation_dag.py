from airflow import DAG 
from airflow.operators.python import PythonOperator 
from airflow.hooks.base import BaseHook 
from datetime import datetime 
import os 

def load_secrets(): 
    conn = BaseHook.get_connection("excel_report_db") 
    
    os.environ["DB_USER"] = conn.login 
    os.environ["DB_PASSWORD"] = conn.password 
    os.environ["DB_HOST"] = conn.host 
    
    os.environ["ENV"] = os.getenv("ENV", "local") 
    
def run_report(): 
    from files_consolidation import run_pipeline 
    run_pipeline() 
    
with DAG( 
    dag_id="excel_report_comparison", 
    start_date=datetime(2024, 1, 1), 
    schedule_interval="@daily", 
    catchup=False, tags=["excel", "reporting", "cyberark"] 
) as dag: 
    
    get_secrets = PythonOperator( 
        task_id="get_secrets", 
        python_callable=load_secrets 
    ) 
    
    generate_report = PythonOperator( 
        task_id="generate_report", 
        python_callable=run_report 
    ) 
    
    get_secrets >> generate_report