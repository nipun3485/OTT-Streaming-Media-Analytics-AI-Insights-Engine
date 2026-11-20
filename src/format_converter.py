from pyspark.sql import SparkSession
import os
import time

def benchmark_format_conversion():
    spark = SparkSession.builder \
        .appName("OTT_Format_Optimizer") \
        .master("local[*]") \
        .getOrCreate()

    csv_path = os.path.abspath("hdfs_root/raw_logs/*.csv")
    df = spark.read.option("header", "true").csv(f"file:///{csv_path}")

    parquet_out = os.path.abspath("hdfs_root/optimized_parquet/watch_logs.parquet")

    # 1. Benchmark Parquet Conversion Write
    start_time = time.time()
    df.write.mode("overwrite").parquet(f"file:///{parquet_out}")
    parquet_write_time = time.time() - start_time

    # 2. Benchmark Reading Speed
    start_time = time.time()
    parquet_df = spark.read.parquet(f"file:///{parquet_out}")
    parquet_df.filter(parquet_df.device_type == "Smart TV").count()
    parquet_read_time = time.time() - start_time

    print("\n⚡ --- Format Optimization Benchmark Results (Exp 6) ---")
    print(f"⏱️ Parquet Write Time: {parquet_write_time:.4f} seconds")
    print(f"⏱️ Column Filter Query Time (Parquet): {parquet_read_time:.4f} seconds")
    print("✅ Parquet provides columnar compression & fast selective querying over CSV.")

    spark.stop()

if __name__ == "__main__":
    benchmark_format_conversion()
