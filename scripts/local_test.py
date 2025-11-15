from logger import get_logger
from utils import filter_incremental
from pyspark.sql import SparkSession

logger = get_logger("local-test")
spark = SparkSession.builder.master("local").appName("local-etl").getOrCreate()

df = spark.read.json("sample_data/input_data.json")
logger.info(f"Loaded {df.count()} local records")
