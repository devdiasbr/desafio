import argparse
from src import utils
from src import bronze
from src import silver
from src import gold

def main():
    parser = argparse.ArgumentParser(description="Executar pipeline de vendas localmente.")
    parser.add_argument("--clean", action="store_true", help="Limpar diretórios de dados antes de executar.")
    
    # Usar parse_known_args para evitar erros com argumentos internos do Databricks/Jupyter (ex: -f kernel.json)
    args, unknown = parser.parse_known_args()
    
    if unknown and not utils.is_databricks():
        print(f"Aviso: Argumentos desconhecidos ignorados: {unknown}")

    # Inicializar Spark Session única para todo o pipeline
    spark = utils.get_spark_session("VendasPipeline")

    if args.clean:
        print("Limpando diretórios de dados...")
        utils.clean_directories()
    else:
        # Garante recursos (Pastas locais ou Database no Databricks)
        utils.ensure_resources(spark)

    try:
        # Executar Camada Bronze
        bronze.run_bronze(spark)
        
        # Executar Camada Silver
        silver.run_silver(spark)
        
        # Executar Camada Gold
        gold.run_gold(spark)
        
        print("\nPipeline executado com sucesso!")
        
    except Exception as e:
        print(f"\nErro durante a execução do pipeline: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
