import airflow 
from airflow import DAG
from datetime import timedelta 
from airflow.utils.dates import days_ago 
from airflow.operators.dagrun_operator import TriggerDagRunOperator 

ARGS = {
    "owner": "Cristina Santos",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Definindo a Dag pai 
with DAG(
    dag_id = "parent_dag",
    schedule_interval = "0 5 * * *",
    description = "Dag pai que vai executar a dag pyspark(ingestao) e bigquey(bronze,silve,gold)",
    default_args = ARGS,
    catchup = False,
    tags = ["parent","orchestration","etl"]
) as dag:
    
    # Tarefa para adicionar um DAG do PySpark
    trigger_pyspark_dag = TriggerDagRunOperator(
        task_id  =  "trigger_pyspark_dag",
        trigger_dag_id = "pyspark_dag",
       
    )

    # Tarefa pra adicionar um DAG no BigQuery
    trigger_bigquery_dag = TriggerDagRunOperator(
        task_id = "trigger_bigquery_dag",
        trigger_dag_id = "bigquery_dag",
        
    )

trigger_pyspark_dag >> trigger_bigquery_dag

