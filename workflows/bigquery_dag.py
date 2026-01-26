import airflow
from airflow import DAG 
from datetime import timedelta 
from airflow.utils.dates import days_ago 
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Definir as variaveis
PROJECT_ID = "projeto-e-commerce-484617"
LOCATION = "US"
SQL_FILE_PATH_1 = "/home/airflow/gcs/data/bigquery/bronzeTable.sql"
SQL_FILE_PATH_2 = "/home/airflow/gcs/data/bigquery/silverTable.sql"
SQL_FILE_PATH_3 = "/home/airflow/gcs/data/bigquery/goldTable.sql"

# função para ler consulta SQL de um arquivo

def read_sql_file(file_path):
    with open(file_path,"r") as f:
        return f.read()

BRONZE_QUERY = read_sql_file(SQL_FILE_PATH_1)
SILVER_QUERY = read_sql_file(SQL_FILE_PATH_2)
GOLD_QUERY = read_sql_file(SQL_FILE_PATH_3)

ARGS = {
    "owner": "Cristina Santos",
    "start_date": None,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id = "bigquery_dag",
    description = "Dag pra criar tabelas na camada bronze,silver e gold",
    schedule_interval = None,
    catchup= False,
    default_args = ARGS,
    tags = ["gcs","bigquery","etl"]
) as dag:
    
    # Tarefa para criar a tabela na bronze
    bronze_table = BigQueryInsertJobOperator(
        task_id = "bronze_table",
        configuration = {
            "query": {
                "query":BRONZE_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Tarefa para criar a tabela silver 
    silver_table = BigQueryInsertJobOperator(
        task_id = "silver_table",
        configuration = {
            "query":{
                "query":SILVER_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Tarefa para criar tabela na camada Gold
    gold_table = BigQueryInsertJobOperator(
        task_id = "gold_table",
        configuration = {
            "query":{
                "query":GOLD_QUERY,
                "useLegacySql": False,
                "priority":"BATCH",
            }
        },

    )

bronze_table >> silver_table >> gold_table