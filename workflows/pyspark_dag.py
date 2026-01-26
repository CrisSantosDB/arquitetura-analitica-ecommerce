import airflow 
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import(
    DataprocStartClusterOperator,
    DataprocStopClusterOperator,
    DataprocSubmitJobOperator,
)

# Define Variaveis 
PROJECT_ID = "projeto-e-commerce-484617"
REGION = "us-central1"
CLUSTER_NAME = "demo-cluster"
COMPOSER_BUCKET = ""

GCS_JOB_FILE = f"gs://{COMPOSER_BUCKET}data/ingestao/IngestaoPostgresqlToLanding.py"
PYSPARK_JOB = {
    "reference":{"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE }
}

ARGS = {
    "owner":"Cristina Santos",
    "start_date": None,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
    }

# Define a Dag
with DAG(
    dag_id = "pyspark_dag",
    description = "DAG para iniciar um cluster Dataproc, executar tarefas PySpark e parar o cluster.",
    schedule_interval= None,
    catchup = False,
    default_args = ARGS,
    tags = ["pyspark","dataproc","etl"]
) as dag:
    # Define as task
    start_cluster = DataprocStartClusterOperator (
        task_id = "start_cluster",
        project_id = PROJECT_ID,
        region = REGION,
        cluster_name = CLUSTER_NAME,
)
    # Submeter e executar job PySpark no cluster Dataproc
    pyspark_job = DataprocSubmitJobOperator(
        task_id = "pyspark_job",
        job = PYSPARK_JOB,
        region = REGION,
        project_id = PROJECT_ID
)

    # Parar o Cluster
    stop_cluster = DataprocStopClusterOperator (
        task_id = "stop_cluster",
        project_id = PROJECT_ID,
        region = REGION,
        cluster_name = CLUSTER_NAME
)

# Definindo dependencias das tasks
start_cluster >> pyspark_job >> stop_cluster