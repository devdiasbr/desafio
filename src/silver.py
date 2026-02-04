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
    if not utils.delta_exists(spark, bronze_path):
        print("Tabela Bronze não encontrada. Execute a camada Bronze primeiro.")
        return

    df_bronze = utils.read_delta(spark, bronze_path)
    
    if df_bronze.count() == 0:
        print("Tabela Bronze vazia.")
        return

    # Transformações: remover duplicatas baseadas em codigo_venda (assumindo único)
    window_spec = Window.partitionBy("codigo_venda").orderBy(col("data_carga").desc())
    df_silver = df_bronze.withColumn("row_num", row_number().over(window_spec)) \
                         .filter(col("row_num") == 1) \
                         .drop("row_num")
    
    # Verifica se a tabela Silver já existe
    if utils.delta_exists(spark, silver_path):
        print("Realizando MERGE na Silver...")
        # MERGE INTO Silver
        df_silver.createOrReplaceTempView("temp_silver")
        
        target_table = utils.get_sql_target(silver_path)
        
        spark.sql(f"""
        MERGE INTO {target_table} AS target
        USING temp_silver AS source
        ON target.codigo_venda = source.codigo_venda
        WHEN MATCHED THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *
        """)
    else:
        print("Criando tabela Silver inicial...")
        utils.write_delta(df_silver, silver_path, partitionBy="timestamp_venda")
    
    print("Camada Silver atualizada.")
    
    # Validação
    print("Validando camada Silver...")
    df_silver_check = utils.read_delta(spark, silver_path)
    df_silver_check.show(10)
    print(f"Total de registros na Silver: {df_silver_check.count()}")

if __name__ == "__main__":
    spark = utils.get_spark_session("SilverLayer")
    run_silver(spark)
