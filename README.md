# OTT Streaming Media Analytics & AI Insights Engine

An end-to-end Big Data engineering pipeline and Generative AI reporting dashboard built with **Apache PySpark**, **simulated HDFS**, **MySQL**, **AWS S3**, **Streamlit**, and the **Google Gemini API (`gemini-2.5-flash`)**.

Developed as a laboratory project consolidating ten core Big Data experiments into a single unified software workflow.

---

## Project Team & Academic Details

* **Institution:** School of Computer Science, University of Petroleum and Energy Studies (UPES), Dehradun
* **Course:** Introduction to Big Data and AI Systems Laboratory
* **Developers:**
  * **Nipun Singh** (SAP ID: `590029310`)
  * **Manasvi Kaushal** (SAP ID: `590023137`)
* **Submitted To:** Prof. Vishal Kumar Lohan

---

## System Architecture

The pipeline processes OTT platform telemetry and relational catalog data across five sequential stages:

1. **Data Generation & Relational Ingestion:** Synthesizes high-velocity streaming telemetry (`play_start`, `pause`, `buffer_wait`) and user reviews while importing structured movie metadata from MySQL via Spark JDBC.
2. **Distributed Storage & Fault Tolerance:** Simulates Hadoop Distributed File System (HDFS) block replication ($RF=2$) across isolated DataNodes (`datanode_1` and `datanode_2`) with automated failover recovery.
3. **Distributed Compute (PySpark MapReduce):** Aggregates playback metrics by title/genre and performs tokenized WordCount sentiment analysis on unstructured user reviews.
4. **Storage Optimization & Cloud Sync:** Converts row-oriented CSV logs into columnar **Apache Parquet** format (~77–80% disk reduction) and syncs partitions with AWS S3 via `boto3`.
5. **Generative AI & Executive Dashboard:** Feeds aggregated metrics into **Google Gemini (`gemini-2.5-flash`)** to generate executive content strategy digests, visualized in an interactive multi-tab **Streamlit** web app.

---

## Laboratory Experiment Mapping

| Exp. | Lab Topic | Module | Implementation Summary |
| :--- | :--- | :--- | :--- |
| **1** | Hadoop & Spark Setup | Environment | Configured OpenJDK, PySpark, MySQL JDBC connector, and Python virtual environment on WSL2. |
| **2** | HDFS Architecture & Failover | `src/03_hdfs_manager.py` | Replicates partitioned blocks ($RF=2$) across two DataNodes and recovers deleted chunks. |
| **3** | MapReduce Data Aggregation | `src/04_mapreduce_analytics.py` | Computes `play_start` counts per `movie_id` to rank top-performing titles. |
| **4** | MapReduce WordCount | `src/04_mapreduce_analytics.py` | Tokenizes unstructured viewer reviews to extract dominant sentiment and QoS keywords. |
| **5** | RDBMS to HDFS Migration | `src/02_rdbms_migrator.py` | Migrates MySQL `movies` catalog table into HDFS Parquet format via Spark JDBC. |
| **6** | File Formats (CSV vs. Parquet) | `src/05_format_converter.py` | Converts raw CSV telemetry into columnar Parquet and benchmarks storage savings. |
| **7** | High-Velocity Log Ingestion | `src/01_log_generator.py` | Generates configurable batches of timestamped streaming events across device types. |
| **8** | Cloud Data Lake Integration | `src/06_s3_exporter.py` | Uploads optimized Parquet partitions to Amazon S3 using the `boto3` SDK. |
| **9** | End-to-End ETL Pipeline | `main_pipeline.py` | Orchestrates sequential execution across ingestion, storage, compute, and AI stages. |
| **10** | AI-Driven Big Data Insights | `src/07_ai_recommender.py` | Generates executive content strategy recommendations via `google-genai` SDK. |

---

## Directory Structure

```text
Sem_3/
├── hdfs_root/                  # Local simulated HDFS data lake (ignored in Git)
│   ├── raw_logs/
│   ├── datanode_1/
│   ├── datanode_2/
│   ├── rdbms_exports/
│   ├── optimized_parquet/
│   └── mr_output/
├── src/
│   ├── 01_log_generator.py
│   ├── 02_rdbms_migrator.py
│   ├── 03_hdfs_manager.py
│   ├── 04_mapreduce_analytics.py
│   ├── 05_format_converter.py
│   ├── 06_s3_exporter.py
│   └── 07_ai_recommender.py
├── main_pipeline.py            # CLI end-to-end ETL orchestrator
├── app.py                      # Interactive Streamlit dashboard
├── requirements.txt            # Python dependencies
├── .gitignore
└── README.md
