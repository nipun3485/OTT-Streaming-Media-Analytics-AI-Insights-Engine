import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, count

def run_mapreduce_jobs():
    spark = SparkSession.builder \
        .appName("OTT_MapReduce_Analytics") \
        .master("local[*]") \
        .getOrCreate()

    logs_path = os.path.abspath("hdfs_root/raw_logs/*.csv")
    df = spark.read.option("header", "true").csv(f"file:///{logs_path}")

    # --- Exp 3: Stream Frequency Analytics ---
    print("\n📊 Computing Top Streamed Content (Exp 3)...")
    top_movies = df.filter(col("event_type") == "play_start") \
                   .groupBy("movie_id") \
                   .agg(count("event_type").alias("total_streams")) \
                   .orderBy(col("total_streams").desc())
    
    top_movies.show(5)

    # --- Exp 4: Review Sentiment WordCount (MapReduce style) ---
    print("\n📝 Running MapReduce WordCount on Customer Reviews (Exp 4)...")
    words_df = df.select(explode(split(lower(col("review_text")), r"\W+")).alias("word"))
    
    stop_words = ["the", "in", "was", "and", "a", "of", "to", "for", "is", "on"]
    filtered_words = words_df.filter(~col("word").isin(stop_words) & (col("word") != ""))

    word_counts = filtered_words.groupBy("word") \
                                .count() \
                                .orderBy(col("count").desc())
    
    word_counts.show(10)

    out_path = os.path.abspath("hdfs_root/mr_output/word_count_summary")
    word_counts.write.mode("overwrite").csv(f"file:///{out_path}")
    print(f"✅ MapReduce Analytics saved to -> {out_path}")

    spark.stop()

if __name__ == "__main__":
    run_mapreduce_jobs()
