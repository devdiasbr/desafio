import argparse
from src import utils
from src import bronze
from src import silver
from src import gold

def main():
    parser = argparse.ArgumentParser(description="Executar pipeline de vendas localmente.")
    parser.add_argument("--clean", action="store_true", help="Limpar diretórios de dados antes de executar.")
    args = parser.parse_args()

    if args.clean:
        print("Limpando diretórios de dados...")
        utils.clean_directories()
    else:
        utils.ensure_directories()

    # Inicializar Spark Session única para todo o pipeline
    spark = utils.get_spark_session("VendasPipeline")

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
