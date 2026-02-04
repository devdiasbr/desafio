import os
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from src import utils

def run_bronze(spark):
    print("Iniciando camada Bronze...")
    
    # Definir schema dos dados
    schema = StructType([
        StructField("codigo_venda", StringType(), True),
        StructField("numero_fiscal", IntegerType(), True),
        StructField("id_produto", IntegerType(), True),
        StructField("nome_produto", StringType(), True),
        StructField("valor", DoubleType(), True),
        StructField("timestamp_venda", TimestampType(), True)
    ])

    # Caminho para os dados
    data_path = utils.DATA_RAW_PATH
    bronze_path = utils.BRONZE_PATH
    processed_files_path = utils.PROCESSED_FILES_PATH

    # Garantir que diretórios existam
    utils.ensure_directories()

    # Listar arquivos na pasta
    all_files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
    processed_files = set()
    
    # Verificar arquivos já processados lendo diretamente da tabela Delta (Idempotência)
    try:
        # Tenta ler a tabela Bronze existente
        df_processed = spark.read.format("delta").load(bronze_path).select("nome_arquivo").distinct()
        rows = df_processed.collect()
        # Extrai apenas o nome do arquivo (basename) do caminho completo
        processed_files = {os.path.basename(r["nome_arquivo"]) for r in rows}
        print(f"Arquivos já processados encontrados na Bronze: {len(processed_files)}")
    except Exception:
        # Tabela não existe (primeira execução) ou erro de acesso
        print("Tabela Bronze não encontrada ou vazia. Processando todos os arquivos.")
        processed_files = set()

    new_files = [f for f in all_files if f not in processed_files]

    if new_files:
        # Ler apenas arquivos novos
        # Spark local path needs explicit protocol or valid path
        file_paths = [os.path.join(data_path, f) for f in new_files]
        df = spark.read.schema(schema).csv(file_paths)
        
        # Adicionar colunas
        df_bronze = df.withColumn("data_carga", current_timestamp())
        
        # Adicionar nome do arquivo usando helper híbrido (suporte a Databricks Shared/UC)
        df_bronze = utils.add_filename_column(df_bronze, "nome_arquivo")
        
        # Salvar em Delta (append para incremental)
        df_bronze.write.format("delta").mode("append").save(bronze_path)
        
        print(f"Processados {len(new_files)} arquivos novos.")
    else:
        print("Nenhum arquivo novo para processar.")

    # Validação
    print("Validando camada Bronze...")
    df_bronze_check = spark.read.format("delta").load(bronze_path)
    df_bronze_check.show(10)
    print(f"Total de registros na Bronze: {df_bronze_check.count()}")

if __name__ == "__main__":
    spark = utils.get_spark_session("BronzeLayer")
    run_bronze(spark)
