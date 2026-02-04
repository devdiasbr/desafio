from pyspark.sql.functions import col, month, year, sum as spark_sum
from delta.tables import DeltaTable
from src import utils

def run_gold(spark):
    print("Iniciando camada Gold...")
    
    silver_path = utils.SILVER_PATH
    gold_fato_path = utils.GOLD_FATO_PATH
    gold_agg_path = utils.GOLD_AGG_PATH
    
    # Ler dados da Silver
    if not utils.delta_exists(spark, silver_path):
        print("Tabela Silver não encontrada. Execute a camada Silver primeiro.")
        return

    df_silver = utils.read_delta(spark, silver_path)
    
    if df_silver.count() == 0:
        print("Tabela Silver vazia.")
        return

    # Fato de Vendas: detalhada
    df_fato = df_silver.select("codigo_venda", "numero_fiscal", "id_produto", "nome_produto", "valor", "timestamp_venda", "data_carga")
    
    # Paths for SQL
    target_fato = utils.get_sql_target(gold_fato_path)
    target_agg = utils.get_sql_target(gold_agg_path)

    # MERGE INTO Fato
    if utils.delta_exists(spark, gold_fato_path):
        print("Realizando MERGE na Fato Vendas...")
        df_fato.createOrReplaceTempView("temp_fato")
        spark.sql(f"""
        MERGE INTO {target_fato} AS target
        USING temp_fato AS source
        ON target.codigo_venda = source.codigo_venda
        WHEN MATCHED THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *
        """)
    else:
        print("Criando tabela Fato Vendas inicial...")
        utils.write_delta(df_fato, gold_fato_path)

    # Tabela Agregada: somatório por id_produto e mês
    df_agg = df_silver.groupBy("id_produto", "nome_produto", year("timestamp_venda").alias("ano"), month("timestamp_venda").alias("mes")) \
                      .agg(spark_sum("valor").alias("valor_total"))
    
    # MERGE INTO Agregada
    if utils.delta_exists(spark, gold_agg_path):
        print("Realizando MERGE na Vendas Agregadas...")
        df_agg.createOrReplaceTempView("temp_agg")
        spark.sql(f"""
        MERGE INTO {target_agg} AS target
        USING temp_agg AS source
        ON target.id_produto = source.id_produto AND target.ano = source.ano AND target.mes = source.mes
        WHEN MATCHED THEN
          UPDATE SET valor_total = source.valor_total
        WHEN NOT MATCHED THEN
          INSERT *
        """)
    else:
        print("Criando tabela Vendas Agregadas inicial...")
        utils.write_delta(df_agg, gold_agg_path)

    print("Camada Gold atualizada.")
    
    # Validação
    print("Validando camada Gold...")
    df_fato_check = utils.read_delta(spark, gold_fato_path)
    df_fato_check.show(10)
    print(f"Total de registros na Fato: {df_fato_check.count()}")
    
    df_agg_check = utils.read_delta(spark, gold_agg_path)
    df_agg_check.show(10)
    print(f"Total de registros na Agregada: {df_agg_check.count()}")

if __name__ == "__main__":
    spark = utils.get_spark_session("GoldLayer")
    run_gold(spark)
