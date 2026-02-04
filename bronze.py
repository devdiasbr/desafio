# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Bronze: Ingestão de Dados
# MAGIC 
# MAGIC Esta camada é responsável pela ingestão inicial dos dados de vendas a partir dos arquivos CSV.
# MAGIC 
# MAGIC ## Funcionalidades:
# MAGIC - Leitura dos 100 arquivos CSV da pasta dados_vendas
# MAGIC - Adição de colunas: data_carga e nome_arquivo
# MAGIC - Carga incremental: processa apenas arquivos novos
# MAGIC - Salvamento em formato Delta na camada Bronze

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import os

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
data_path = "/dbfs/mnt/dados_vendas"  # Ajustar conforme necessário
bronze_path = "/dbfs/mnt/bronze/vendas"

# Para carga incremental, verificar arquivos já processados
processed_files_path = "/dbfs/mnt/bronze/processed_files"
if not os.path.exists(processed_files_path):
    os.makedirs(processed_files_path)

# Listar arquivos na pasta
all_files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
processed_files = set()
if os.path.exists(f"{processed_files_path}/processed.txt"):
    with open(f"{processed_files_path}/processed.txt", 'r') as f:
        processed_files = set(f.read().splitlines())

new_files = [f for f in all_files if f not in processed_files]

if new_files:
    # Ler apenas arquivos novos
    df = spark.read.schema(schema).csv([f"{data_path}/{f}" for f in new_files])
    
    # Adicionar colunas
    df_bronze = df.withColumn("data_carga", current_timestamp()) \
                  .withColumn("nome_arquivo", input_file_name())
    
    # Salvar em Delta (append para incremental)
    df_bronze.write.format("delta").mode("append").save(bronze_path)
    
    # Registrar arquivos processados
    with open(f"{processed_files_path}/processed.txt", 'a') as f:
        for file in new_files:
            f.write(file + '\n')
    
    print(f"Processados {len(new_files)} arquivos novos.")
else:
    print("Nenhum arquivo novo para processar.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação
# MAGIC Verificar se os dados foram carregados corretamente.

# COMMAND ----------

# Verificar dados na camada Bronze
df_bronze_check = spark.read.format("delta").load(bronze_path)
df_bronze_check.show(10)
print(f"Total de registros: {df_bronze_check.count()}")