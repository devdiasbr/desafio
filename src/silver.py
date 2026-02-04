from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from src import utils

def run_silver(spark):
    print("Iniciando camada Silver...")
    
    bronze_path = utils.BRONZE_PATH
    silver_path = utils.SILVER_PATH
    
    # Ler dados da Bronze
    # Verifica se existe dados na bronze antes
    if not DeltaTable.isDeltaTable(spark, bronze_path):
        print("Tabela Bronze não encontrada. Execute a camada Bronze primeiro.")
        return

    df_bronze = spark.read.format("delta").load(bronze_path)
    
    if df_bronze.count() == 0:
        print("Tabela Bronze vazia.")
        return

    # Transformações: remover duplicatas baseadas em codigo_venda (assumindo único)
    window_spec = Window.partitionBy("codigo_venda").orderBy(col("data_carga").desc())
    df_silver = df_bronze.withColumn("row_num", row_number().over(window_spec)) \
                         .filter(col("row_num") == 1) \
                         .drop("row_num")
    
    # Verifica se a tabela Silver já existe
    if DeltaTable.isDeltaTable(spark, silver_path):
        print("Realizando MERGE na Silver...")
        # MERGE INTO Silver
        df_silver.createOrReplaceTempView("temp_silver")
        
        # Note: Delta paths in SQL need `delta.` prefix and backticks usually
        # But for local paths with spaces or special chars, absolute path is best.
        # Ensure path is formatted correctly for SQL
        # We need to escape backslashes on Windows for SQL strings if they exist
        silver_path_sql = silver_path.replace("\\", "/")
        
        spark.sql(f"""
        MERGE INTO delta.`{silver_path_sql}` AS target
        USING temp_silver AS source
        ON target.codigo_venda = source.codigo_venda
        WHEN MATCHED THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *
        """)
    else:
        print("Criando tabela Silver inicial...")
        df_silver.write.format("delta").partitionBy("timestamp_venda").save(silver_path)
    
    print("Camada Silver atualizada.")
    
    # Validação
    print("Validando camada Silver...")
    df_silver_check = spark.read.format("delta").load(silver_path)
    df_silver_check.show(10)
    print(f"Total de registros na Silver: {df_silver_check.count()}")

if __name__ == "__main__":
    spark = utils.get_spark_session("SilverLayer")
    run_silver(spark)
