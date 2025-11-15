import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

from logger import get_logger
from utils import filter_incremental, write_partitioned

args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path", "incremental_column"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

logger = get_logger("glue-job")
logger.info("Starting Glue ETL job...")

job.init(args["JOB_NAME"], args)
df = spark.read.format("json").load(args["input_path"])

bookmark_value = glueContext.get_bookmark(args["incremental_column"])
df_inc = filter_incremental(df, args["incremental_column"], bookmark_value)

logger.info(f"Processing {df_inc.count()} incremental records...")

write_partitioned(df_inc, args["output_path"], partition_cols=["year", "month"])
job.commit()
