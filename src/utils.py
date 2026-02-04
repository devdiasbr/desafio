import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, col

def is_databricks():
    """Verifica se o código está rodando no Databricks."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

def get_spark_session(app_name="DesafioLocal"):
    if is_databricks():
        # No Databricks, a sessão já existe ou é gerenciada automaticamente
        try:
             # Tenta obter a sessão ativa existente
             return SparkSession.builder.getOrCreate()
        except Exception:
             # Se falhar, tenta criar uma nova (útil se a anterior morreu)
             return SparkSession.builder.appName(app_name).getOrCreate()
    
    # Configuração para execução local
    from delta import configure_spark_with_delta_pip
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.warehouse.dir", os.path.abspath("./spark-warehouse"))

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # Silenciar logs verbosos e erros específicos de Windows
    spark.sparkContext.setLogLevel("ERROR")
    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager")
    logger.setLevel(log4j.Level.OFF)
    
    return spark

# Define paths
# A lógica de os.path.dirname(os.path.dirname(...)) garante que pegamos a raiz do projeto (desafio/)
# assumindo que este arquivo está em desafio/src/utils.py
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if is_databricks():
    # No Databricks, usamos Tabelas Gerenciadas (Managed Tables)
    # Isso delega ao Databricks a escolha do local de armazenamento (Hive/Unity Catalog)
    # e evita erros de permissão em sistemas de arquivos somente leitura ou bloqueados (Public DBFS)
    
    # DATA_RAW_PATH continua no Workspace (assumindo que os CSVs estão lá e leitura é permitida)
    DATA_RAW_PATH = os.path.join(BASE_DIR, "dados_vendas")
    
    # Nome do Database específico para o projeto
    DATABASE_NAME = "desafio_beca"

    # Nomes das tabelas (com prefixo do database)
    BRONZE_PATH = f"{DATABASE_NAME}.bronze_vendas"
    SILVER_PATH = f"{DATABASE_NAME}.silver_vendas"
    GOLD_FATO_PATH = f"{DATABASE_NAME}.gold_fato_vendas"
    GOLD_AGG_PATH = f"{DATABASE_NAME}.gold_vendas_agregadas"
    PROCESSED_FILES_PATH = f"{DATABASE_NAME}.bronze_processed_files" 

else:
    DATABASE_NAME = None # Não usado localmente da mesma forma (usa arquivos)
    # Configure Hadoop for Windows (apenas local)
    HADOOP_HOME = os.path.join(BASE_DIR, "hadoop")
    os.environ["HADOOP_HOME"] = HADOOP_HOME
    os.environ["PATH"] += os.pathsep + os.path.join(HADOOP_HOME, "bin")

    DATA_RAW_PATH = os.path.join(BASE_DIR, "dados_vendas")
    BRONZE_PATH = os.path.join(BASE_DIR, "data", "bronze", "vendas")
    SILVER_PATH = os.path.join(BASE_DIR, "data", "silver", "vendas")
    GOLD_FATO_PATH = os.path.join(BASE_DIR, "data", "gold", "fato_vendas")
    GOLD_AGG_PATH = os.path.join(BASE_DIR, "data", "gold", "vendas_agregadas")
    PROCESSED_FILES_PATH = os.path.join(BASE_DIR, "data", "bronze", "processed_files")

# Ensure directories exist
def ensure_directories():
    pass # Mantido para compatibilidade, mas a lógica real está abaixo se precisar de algo específico
    
def ensure_database(spark):
    """Garante que o database exista no Databricks."""
    if is_databricks():
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
        # Opcional: Definir como atual para simplificar consultas manuais depois
        spark.sql(f"USE {DATABASE_NAME}")

def ensure_resources(spark=None):
    """Função unificada para garantir recursos (diretórios locais ou database remoto)."""
    if is_databricks():
        if spark:
            ensure_database(spark)
    else:
        os.makedirs(BRONZE_PATH, exist_ok=True)
        os.makedirs(SILVER_PATH, exist_ok=True)
        os.makedirs(GOLD_FATO_PATH, exist_ok=True)
        os.makedirs(GOLD_AGG_PATH, exist_ok=True)
        os.makedirs(PROCESSED_FILES_PATH, exist_ok=True)

# Clean up for fresh run (optional, for testing)
def clean_directories():
    if is_databricks():
        # No Databricks, clean seria dropar tabelas. Cuidado ao usar em produção.
        pass
    else:
        if os.path.exists(os.path.join(BASE_DIR, "data")):
            # On Windows, ignore_errors=True helps avoid permission errors with open files
            shutil.rmtree(os.path.join(BASE_DIR, "data"), ignore_errors=True)
        ensure_directories()

# --- Helpers Híbridos (Local Path vs Managed Table) ---

def is_path(identifier):
    """Verifica se o identificador parece ser um caminho de arquivo."""
    return "/" in identifier or "\\" in identifier or ":" in identifier

def read_delta(spark, identifier):
    """Lê Delta table ou path de forma consistente."""
    if is_path(identifier):
        return spark.read.format("delta").load(identifier)
    else:
        return spark.read.table(identifier)

def write_delta(df, identifier, mode="append", partitionBy=None):
    """Escreve Delta table ou path de forma consistente."""
    writer = df.write.format("delta").mode(mode)
    if partitionBy:
        writer = writer.partitionBy(partitionBy)
        
    if is_path(identifier):
        writer.save(identifier)
    else:
        writer.saveAsTable(identifier)

def get_sql_target(identifier):
    """Retorna a cláusula correta para SQL (delta.`path` ou table_name)."""
    if is_path(identifier):
        clean_path = identifier.replace("\\", "/")
        return f"delta.`{clean_path}`"
    else:
        return identifier

def delta_exists(spark, identifier):
    """Verifica se a tabela/path Delta existe."""
    from delta.tables import DeltaTable
    try:
        if is_path(identifier):
            return DeltaTable.isDeltaTable(spark, identifier)
        else:
            return spark.catalog.tableExists(identifier)
    except Exception:
        return False


# Helper para adicionar coluna de nome de arquivo (Compatibilidade Híbrida)
def add_filename_column(df, col_name="nome_arquivo"):
    """
    Adiciona uma coluna com o nome do arquivo de origem.
    Usa _metadata.file_path no Databricks (suporte a UC/Shared) e input_file_name() localmente.
    """
    if is_databricks():
        # No Databricks com modo Shared/Unity Catalog, input_file_name() é bloqueado.
        # Deve-se usar a coluna de metadados _metadata.file_path
        try:
            return df.withColumn(col_name, col("_metadata.file_path"))
        except Exception:
            # Fallback caso _metadata não esteja disponível (versões antigas ou formatos não suportados)
            return df.withColumn(col_name, input_file_name())
    else:
        # Execução local padrão
        return df.withColumn(col_name, input_file_name())

def get_dbutils(spark):
    """Retrieve DBUtils in Databricks or mimic it locally."""
    if is_databricks():
        try:
            from pyspark.dbutils import DBUtils
            return DBUtils(spark)
        except ImportError:
            # Fallback for environments where pyspark.dbutils is not available
            return None
    return None

def list_files(path):
    """List files in a directory, supporting both local OS and DBFS paths."""
    if path.startswith("dbfs:"):
        # Use DBUtils for DBFS paths
        spark = get_spark_session()
        dbutils = get_dbutils(spark)
        if dbutils:
            try:
                files = dbutils.fs.ls(path)
                return [f.name for f in files]
            except Exception as e:
                print(f"Error listing DBFS path {path}: {e}")
                return []
        else:
            print("DBUtils not available for DBFS path listing.")
            return []
    else:
        # Local filesystem
        try:
            return os.listdir(path)
        except OSError as e:
            print(f"Error listing local path {path}: {e}")
            return []
