import sys
import os

sys.path.append(os.path.abspath("src"))

from log_generator import generate_watch_logs
from rdbms_migrator import migrate_mysql_to_hdfs  # <--- Added
from hdfs_manager import simulate_hdfs_replication_and_failover
from mapreduce_analytics import run_mapreduce_jobs
from format_converter import benchmark_format_conversion
from ai_recommender import generate_ai_insights

def run_end_to_end_pipeline():
    print("=" * 60)
    print("🚀 STARTING OTT BIG DATA BATCH PIPELINE SIMULATION")
    print("=" * 60)

    # Step 1: Log Simulation (Exp 7)
    print("\n[STEP 1/6] Ingesting Stream Log Data...")
    raw_file = generate_watch_logs(2000)

    # Step 2: MySQL Migration (Exp 5)
    print("\n[STEP 2/6] Migrating MySQL Relational Data to HDFS...")
    migrate_mysql_to_hdfs()

    # Step 3: HDFS Replication (Exp 1 & 2)
    print("\n[STEP 3/6] Testing HDFS Block Replication & Fault Recovery...")
    simulate_hdfs_replication_and_failover(raw_file)

    # Step 4: MapReduce Processing (Exp 3 & 4)
    print("\n[STEP 4/6] Executing MapReduce Analytics Jobs...")
    run_mapreduce_jobs()

    # Step 5: Format Optimization (Exp 6)
    print("\n[STEP 5/6] Converting CSV Datasets to Parquet Format...")
    benchmark_format_conversion()

    # Step 6: AI Integration (Exp 10)
    print("\n[STEP 6/6] Generating AI Insights via LLM API...")
    generate_ai_insights(
        top_genres=["Sci-Fi", "Comedy"],
        review_keywords=["unexpected twist", "buffering", "visuals insane"]
    )

    print("\n" + "=" * 60)
    print("🎉 PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
    print("=" * 60)

if __name__ == "__main__":
    run_end_to_end_pipeline()
