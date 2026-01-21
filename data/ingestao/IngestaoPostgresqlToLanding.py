from google.cloud import storage, bigquery
from google.cloud import secretmanager
import pandas as pd 
from pyspark.sql import SparkSession
import datetime
import json

# Criar uma SparkSession
spark = (SparkSession.builder 
    .appName("ECommercePostgresqlToLanding") 
    .getOrCreate())


# Variaveis do GCS
GCS_BUCKET = "datalake-ecommerce-2026"
LANDING_PATH = f"Landing/Ecommerce_DB/"
ARCHIVE_PATH= "Landing/Archive/"
CONFIG_FILE_PATH =f"gs://{GCS_BUCKET}/configs/ecommerce_config.csv"

# Configuração do BigQuery
BQ_PROJETO = "projeto-e-commerce-484617"
BQ_AUDIT_TABLE = f"{BQ_PROJETO}.auditoria.tb_auditoria"
BQ_LOG_TABLE = f"{BQ_PROJETO}.logs.tb_log"
#BQ_TEMP_BUCKET = "datalake-ecommerce-2026"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"


# Secret manager
PROJECT_ID = "projeto-e-commerce-484617" 

def get_secret(secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    nome = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"
    
    response = client.access_secret_version(request={"name": nome})
    return response.payload.data.decode("UTF-8")
print("Tentando acessar o Secret Manager")

try:
    db_user = get_secret("db_user")
    db_password = get_secret("db_password")
    db_host = get_secret("db_host")
    print("✅ SUCESSO")
except Exception as e:
    print(f"❌ Error: {e}")

#Configuração Banco
POSTGRESQL_CONFIG = {
    "url": "jdbc:postgresql://localhost/DB_Prod_ecommerce",
    "driver": "org.postgresql.Driver",
    "user": db_user,
    "password": db_password
}



# Mecanismo de registro
log_entries = [] # Entrada de log lista de registros

# Função de mensagem de evento
def log_event(event_type,message, table=None):
    """ Registra um evento e o armazena na lista de registros(log_entries)"""
    log_entry =  {
        "timestamp":datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message":message,
        "table":table}
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} -{message}")
    
###################################################################################################

# Inicialização GCS & BigQuery Clients
storage_client = storage.Client()
bq_client = bigquery.Client()

# Função para slvar o GCS
def save_logs_to_gcs():
    """Salvar os registros em um arquivo JSON e faz o upload para o GCS."""
    log_filename = f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"
   
    #Criar arquivo
    json_data = json.dumps(log_entries,ensure_ascii=False, indent=2)
    
    #Pegar Bucket GCS
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(log_filepath)
    
    # Carregar dados JSON como um arquivo para o gcs
    blob.upload_from_string(json_data, content_type="application/json")
    
    print(f"✅ Registros Salvos com Sucesso no GCS em gs://{GCS_BUCKET}/{log_filepath}")
    
# Função pra salvar nos registros na tabela de log
def save_logs_to_bigquery():
    """Salva no BigQuery"""
    if log_entries:
        log_df = spark.createDataFrame(log_entries)
        (log_df.write.format("bigquery")
         .option("table",BQ_LOG_TABLE)
         .option("temporaryGcsBucket",BQ_TEMP_PATH)
         .mode("append")
         .save())
        print("✅ Registro armazenados no BigQuery para análise futura")
        
#####################################################################################

# Função para ler o arquivo de configuuração
def read_config_file():
    try:
        df = spark.read.csv(CONFIG_FILE_PATH,header=True)
        log_event("INFO","✅ Arquivo de configuração lido com sucesso")
        return df
    except Exception as e:
              log_event('INFO',f'❌ Arquivo de configuração não lido: {e}')
    

########################################################################################

# Função para mover arquivo criado na landing para o archive
def move_existing_files_to_archive(table):
    prefix = f"{LANDING_PATH}{table}/"
    blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(prefix=prefix))

    if not blobs:
        log_event("INFO", f"❌ Não existe arquivo da table {table}")
        return

    today = datetime.datetime.today().strftime("%Y/%m/%d")

    for blob in blobs:
        source_blob = storage_client.bucket(GCS_BUCKET).blob(blob.name)

        archive_path = (
            f"{ARCHIVE_PATH}{table}/{today}/"
            f"{blob.name.split('/')[-1]}"
        )

        destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)

        storage_client.bucket(GCS_BUCKET).copy_blob(
            source_blob,
            storage_client.bucket(GCS_BUCKET),
            destination_blob.name
        )

        source_blob.delete()

    log_event("INFO", f"📦 Dados da table {table} movidos para archive", table=table)

        
############################################################################################################

# Função pra obter última data processada (watermark) mais recente da tabela de auditotoria do BigQuery

def get_latest_watermark(table_name):
    query = f"""SELECT MAX(load_timestamp) as latest_timestamp
    from `{BQ_AUDIT_TABLE}`
    WHERE tablename = '{table_name}'
    """
    query_job = bq_client.query(query)
    result = query_job.result()
    for row in result:
        return row.latest_timestamp if row.latest_timestamp else "1900-01-01 00:00:00"
    return "1900-01-01 00:00:00"

###########################################################################################

def extract_and_save_to_landing(table,load_type,watermark_col):
    try:
        #Pegar último watermark
        last_watermark = get_latest_watermark(table) if load_type.lower() == "incremental" else None
        log_event("INFO",f"última data processada (watermark) para {table}: '{last_watermark}'", table=table)
        
        # Gerar consulta SQL
        query = (f"(SELECT * FROM {table}) AS t" if load_type.lower() == "full load" else
               f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t")
        
        # Ler os dados no Banco
        df = (spark.read.format("jdbc")
             .option("url",POSTGRESQL_CONFIG["url"])
              .option("user",POSTGRESQL_CONFIG["user"])
              .option("password",POSTGRESQL_CONFIG["password"])
              .option("driver",POSTGRESQL_CONFIG["driver"])
              .option("dbtable",query)
              .load())
        
        log_event("SUCESSO",f"✅ Dados extraídos com sucesso de {table}",table=table)
        
        # Converter DataFrama do Spark para Json 
        # Gerar data para o nome do arquivo
        today = datetime.datetime.today().strftime('%d%m%Y')
        
        # Caminho do GCS
        json_path = f"gs://{GCS_BUCKET}/{LANDING_PATH}{table}"
        
        # Gravar JSON 
        (df
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(json_path)
        )
         
        log_event("SUCESSO", f"✅ Arquivo JSON gravado com sucesso no {json_path}")
        
        # Inserir entrada de auditoria 
        audit_df = spark.createDataFrame([
        (table, load_type, df.count(), datetime.datetime.now(), "SUCESSO")],["tablename","load_type","record_count","load_timestamp","status"])
        (audit_df.write.format("bigquery")
        .option("table",BQ_AUDIT_TABLE)
        .option("temporaryGcsBucket",BQ_TEMP_PATH)
        .mode("append")
         .save())
        
        log_event("SUCESSO", f"✅ Registro de auditoria atualizado para {table}", table=table)
        
    except Exception as e:
        log_event("ERRO",f"❌ Processamento de erros {table}: {str(e)}",table=table)
        
    ###################################################################################################
# Main 
config_df = read_config_file()
for row in config_df.collect():  # ou config_df.toLocalIterator()
    if row["is_active"] == '1':
        database,datasource,tablename,loadtype,watermark,is_active,targetpath = row
        move_existing_files_to_archive(tablename)
        extract_and_save_to_landing(tablename,loadtype,watermark)

save_logs_to_gcs()
save_logs_to_bigquery()
print("✅ Pipeline concluído com sucesso!")

        



