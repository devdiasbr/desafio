import unittest
import os
import sys
import shutil
import tempfile

# Add project root to sys.path to ensure 'src' module can be found
try:
    # Local execution
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
except NameError:
    # Databricks execution (interactive mode where __file__ is not defined)
    project_root = os.getcwd()

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from pyspark.sql import SparkSession
from src import utils
from src import bronze, silver, gold
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from datetime import datetime

class TestPipeline(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a temporary directory for the test execution
        if utils.is_databricks():
            import uuid
            # Use dbfs:/tmp for Databricks to avoid local filesystem restrictions
            # We use DBUtils to create directories and copy files
            cls.test_id = uuid.uuid4().hex
            cls.test_dir = f"dbfs:/tmp/test_pipeline_{cls.test_id}"
            
            # Initialize Spark first to get DBUtils
            # FORCE creation of a new session if the current one is broken
            # This is a workaround for the persistent SESSION_CLOSED error
            try:
                cls.spark = utils.get_spark_session("TestPipeline")
                # Test if session is alive
                cls.spark.sql("SELECT 1").collect()
            except Exception:
                print("Session seems dead, trying to create a new one...")
                from pyspark.sql import SparkSession
                cls.spark = SparkSession.builder.appName("TestPipeline_Recovery").getOrCreate()
            
            cls.dbutils = utils.get_dbutils(cls.spark)
            
            if cls.dbutils:
                try:
                    cls.dbutils.fs.mkdirs(cls.test_dir)
                except Exception as e:
                    print(f"Error creating DBFS dir: {e}")
                    if "SESSION_CLOSED" in str(e) or "INVALID_HANDLE" in str(e):
                        print("\n" + "="*80)
                        print("CRITICAL ERROR: Spark Session is CLOSED.")
                        print("Please RESTART your Databricks Cluster or Detach/Reattach your Notebook.")
                        print("The previous test run might have stopped the session.")
                        print("="*80 + "\n")
                    raise e
            else:
                 raise RuntimeError("DBUtils not available in Databricks environment")
        else:
            cls.test_dir = tempfile.mkdtemp()
            # Initialize Spark
            cls.spark = utils.get_spark_session("TestPipeline")
        
        # Override paths in utils to point to the temp directory
        cls.original_base_dir = utils.BASE_DIR
        # For DBFS, BASE_DIR doesn't make sense as root for everything if we use os.path.join
        # But utils uses it to build default paths. We override specific paths anyway.
        
        # Define paths within the temp dir
        if utils.is_databricks():
            utils.DATA_RAW_PATH = f"{cls.test_dir}/dados_vendas"
            utils.BRONZE_PATH = f"{cls.test_dir}/data/bronze/vendas"
            utils.SILVER_PATH = f"{cls.test_dir}/data/silver/vendas"
            utils.GOLD_FATO_PATH = f"{cls.test_dir}/data/gold/fato_vendas"
            utils.GOLD_AGG_PATH = f"{cls.test_dir}/data/gold/vendas_agregadas"
            utils.PROCESSED_FILES_PATH = f"{cls.test_dir}/data/bronze/processed_files"
            
            # Ensure raw data path exists via dbutils
            cls.dbutils.fs.mkdirs(utils.DATA_RAW_PATH)
            
            # We skip ensure_directories() as it might try os.makedirs on DBFS paths which fails
        else:
            utils.BASE_DIR = cls.test_dir
            utils.DATA_RAW_PATH = os.path.join(cls.test_dir, "dados_vendas")
            utils.BRONZE_PATH = os.path.join(cls.test_dir, "data", "bronze", "vendas")
            utils.SILVER_PATH = os.path.join(cls.test_dir, "data", "silver", "vendas")
            utils.GOLD_FATO_PATH = os.path.join(cls.test_dir, "data", "gold", "fato_vendas")
            utils.GOLD_AGG_PATH = os.path.join(cls.test_dir, "data", "gold", "vendas_agregadas")
            utils.PROCESSED_FILES_PATH = os.path.join(cls.test_dir, "data", "bronze", "processed_files")
            
            # Ensure raw data path exists
            os.makedirs(utils.DATA_RAW_PATH, exist_ok=True)
            utils.ensure_directories()
        

    @classmethod
    def tearDownClass(cls):
        if not utils.is_databricks():
            cls.spark.stop()
            # Restore original paths
            utils.BASE_DIR = cls.original_base_dir
            # Clean up temp dir
            if os.path.exists(cls.test_dir):
                shutil.rmtree(cls.test_dir)
        else:
            # Clean up DBFS temp dir
            try:
                cls.dbutils.fs.rm(cls.test_dir, True)
            except Exception as e:
                print(f"Error cleaning up DBFS: {e}")

    def setUp(self):
        # Clean data directories before each test
        if utils.is_databricks():
             # Clean data subdirectories in DBFS
             data_dir = f"{self.test_dir}/data"
             try:
                 self.dbutils.fs.rm(data_dir, True)
             except:
                 pass
                 
             # Clean raw data directory
             try:
                 self.dbutils.fs.rm(utils.DATA_RAW_PATH, True)
                 self.dbutils.fs.mkdirs(utils.DATA_RAW_PATH)
             except:
                 pass
        else:
            if os.path.exists(os.path.join(self.test_dir, "data")):
                shutil.rmtree(os.path.join(self.test_dir, "data"))
            utils.ensure_directories()
            
            # Clean raw data directory
            if os.path.exists(utils.DATA_RAW_PATH):
                 shutil.rmtree(utils.DATA_RAW_PATH)
            os.makedirs(utils.DATA_RAW_PATH, exist_ok=True)

        # Setup real sample data
        self.setup_real_sample_data()

    def setup_real_sample_data(self):
        # Copy first 5 files from real source to test raw path
        real_source_path = os.path.join(self.original_base_dir, "dados_vendas")
        # Local source files (in the repo)
        files = sorted([f for f in os.listdir(real_source_path) if f.endswith('.csv')])[:5]
        
        if utils.is_databricks():
            for f in files:
                source = f"file:{os.path.join(real_source_path, f)}"
                dest = f"{utils.DATA_RAW_PATH}/{f}"
                self.dbutils.fs.cp(source, dest)
        else:
            for f in files:
                shutil.copy(os.path.join(real_source_path, f), os.path.join(utils.DATA_RAW_PATH, f))
            
    def test_bronze_ingestion(self):
        bronze.run_bronze(self.spark)
        
        df_bronze = self.spark.read.format("delta").load(utils.BRONZE_PATH)
        count = df_bronze.count()
        print(f"Bronze Count: {count}")
        
        self.assertTrue(count > 0)
        self.assertTrue("data_carga" in df_bronze.columns)
        self.assertTrue("nome_arquivo" in df_bronze.columns)

    def test_silver_deduplication(self):
        bronze.run_bronze(self.spark)
        silver.run_silver(self.spark)
        
        df_silver = self.spark.read.format("delta").load(utils.SILVER_PATH)
        count = df_silver.count()
        print(f"Silver Count: {count}")
        
        self.assertTrue(count > 0)
        
        # Bronze might have duplicates, so Silver count should be <= Bronze count
        df_bronze = self.spark.read.format("delta").load(utils.BRONZE_PATH)
        self.assertTrue(count <= df_bronze.count())
        
        # Verify columns
        expected_cols = ["codigo_venda", "numero_fiscal", "id_produto", "nome_produto", "valor", "timestamp_venda"]
        for col in expected_cols:
            self.assertTrue(col in df_silver.columns)

    def test_gold_aggregation(self):
        bronze.run_bronze(self.spark)
        silver.run_silver(self.spark)
        gold.run_gold(self.spark)
        
        df_agg = self.spark.read.format("delta").load(utils.GOLD_AGG_PATH)
        count = df_agg.count()
        print(f"Gold Agg Count: {count}")
        
        self.assertTrue(count > 0)
        
        # Check if we have aggregation columns
        self.assertTrue("valor_total" in df_agg.columns)
        self.assertTrue("ano" in df_agg.columns)
        self.assertTrue("mes" in df_agg.columns)

if __name__ == '__main__':
    if utils.is_databricks():
        # Limpar argumentos do Databricks para não confundir o unittest
        sys.argv = [sys.argv[0]]
    unittest.main()
