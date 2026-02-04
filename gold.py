# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Gold: Modelagem Analítica
# MAGIC 
# MAGIC Esta camada cria as tabelas analíticas finais.
# MAGIC 
# MAGIC ## Funcionalidades:
# MAGIC - Fato de Vendas: tabela detalhada
# MAGIC - Tabela Agregada: somatório de vendas por produto e mês
# MAGIC - Carga incremental usando MERGE

# COMMAND ----------

from pyspark.sql.functions import col, month, year, sum as spark_sum

silver_path = "/dbfs/mnt/silver/vendas"
gold_fato_path = "/dbfs/mnt/gold/fato_vendas"
gold_agg_path = "/dbfs/mnt/gold/vendas_agregadas"

# Ler dados da Silver
df_silver = spark.read.format("delta").load(silver_path)

# Fato de Vendas: detalhada
df_fato = df_silver.select("codigo_venda", "numero_fiscal", "id_produto", "nome_produto", "valor", "timestamp_venda", "data_carga")

# MERGE INTO Fato
df_fato.createOrReplaceTempView("temp_fato")

spark.sql(f"""
MERGE INTO delta.`{gold_fato_path}` AS target
USING temp_fato AS source
ON target.codigo_venda = source.codigo_venda
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

# Tabela Agregada: somatório por id_produto e mês
df_agg = df_silver.groupBy("id_produto", "nome_produto", year("timestamp_venda").alias("ano"), month("timestamp_venda").alias("mes")) \
                  .agg(spark_sum("valor").alias("valor_total"))

# MERGE INTO Agregada
df_agg.createOrReplaceTempView("temp_agg")

spark.sql(f"""
MERGE INTO delta.`{gold_agg_path}` AS target
USING temp_agg AS source
ON target.id_produto = source.id_produto AND target.ano = source.ano AND target.mes = source.mes
WHEN MATCHED THEN
  UPDATE SET valor_total = source.valor_total
WHEN NOT MATCHED THEN
  INSERT *
""")

print("Camada Gold atualizada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação
# MAGIC Verificar dados nas tabelas Gold.

# COMMAND ----------

df_fato_check = spark.read.format("delta").load(gold_fato_path)
df_fato_check.show(10)
print(f"Total de registros na Fato: {df_fato_check.count()}")

df_agg_check = spark.read.format("delta").load(gold_agg_path)
df_agg_check.show(10)
print(f"Total de registros na Agregada: {df_agg_check.count()}")