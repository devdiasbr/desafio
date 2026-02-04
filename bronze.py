import os
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import local_utils

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
    data_path = local_utils.DATA_RAW_PATH
    bronze_path = local_utils.BRONZE_PATH
    processed_files_path = local_utils.PROCESSED_FILES_PATH

    # Garantir que diretórios existam
    local_utils.ensure_directories()

    # Listar arquivos na pasta
    all_files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
    processed_files = set()
    
    processed_file_record = os.path.join(processed_files_path, "processed.txt")
    if os.path.exists(processed_file_record):
        with open(processed_file_record, 'r') as f:
            processed_files = set(f.read().splitlines())

    new_files = [f for f in all_files if f not in processed_files]

    if new_files:
        # Ler apenas arquivos novos
        # Spark local path needs explicit protocol or valid path
        file_paths = [os.path.join(data_path, f) for f in new_files]
        df = spark.read.schema(schema).csv(file_paths)
        
        # Adicionar colunas
        df_bronze = df.withColumn("data_carga", current_timestamp()) \
                      .withColumn("nome_arquivo", input_file_name())
        
        # Salvar em Delta (append para incremental)
        df_bronze.write.format("delta").mode("append").save(bronze_path)
        
        # Registrar arquivos processados
        with open(processed_file_record, 'a') as f:
            for file in new_files:
                f.write(file + '\n')
        
        print(f"Processados {len(new_files)} arquivos novos.")
    else:
        print("Nenhum arquivo novo para processar.")

    # Validação
    print("Validando camada Bronze...")
    df_bronze_check = spark.read.format("delta").load(bronze_path)
    df_bronze_check.show(10)
    print(f"Total de registros na Bronze: {df_bronze_check.count()}")

if __name__ == "__main__":
    spark = local_utils.get_spark_session("BronzeLayer")
    run_bronze(spark)
