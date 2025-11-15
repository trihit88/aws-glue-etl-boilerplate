from pyspark.sql.functions import col

def filter_incremental(df, column_name, last_value):
    return df.filter(col(column_name) > last_value)

def write_partitioned(df, output_path, partition_cols):
    (df.write.mode("append").partitionBy(partition_cols).parquet(output_path))
