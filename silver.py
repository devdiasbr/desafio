# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Silver: Transformação de Dados
# MAGIC 
# MAGIC Esta camada realiza transformações nos dados da camada Bronze.
# MAGIC 
# MAGIC ## Funcionalidades:
# MAGIC - Leitura dos dados da camada Bronze
# MAGIC - Aplicação de transformações (ex: limpeza, deduplicação)
# MAGIC - Uso de MERGE para carga incremental
# MAGIC - Salvamento em formato Delta com particionamento

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

bronze_path = "/dbfs/mnt/bronze/vendas"
silver_path = "/dbfs/mnt/silver/vendas"

# Ler dados da Bronze
df_bronze = spark.read.format("delta").load(bronze_path)

# Transformações: remover duplicatas baseadas em codigo_venda (assumindo único)
window_spec = Window.partitionBy("codigo_venda").orderBy(col("data_carga").desc())
df_silver = df_bronze.withColumn("row_num", row_number().over(window_spec)) \
                     .filter(col("row_num") == 1) \
                     .drop("row_num")

# MERGE INTO Silver
df_silver.createOrReplaceTempView("temp_silver")

spark.sql(f"""
MERGE INTO delta.`{silver_path}` AS target
USING temp_silver AS source
ON target.codigo_venda = source.codigo_venda
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

# Se a tabela não existir, criar
if not spark.catalog.tableExists("silver_vendas"):
    df_silver.write.format("delta").partitionBy("timestamp_venda").saveAsTable("silver_vendas")

print("Camada Silver atualizada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação
# MAGIC Verificar dados na camada Silver.

# COMMAND ----------

df_silver_check = spark.read.format("delta").load(silver_path)
df_silver_check.show(10)
print(f"Total de registros: {df_silver_check.count()}")