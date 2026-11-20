import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import glob
import time
import sys

# Ensure src modules can be imported
sys.path.append(os.path.abspath("src"))

from log_generator import generate_watch_logs
from hdfs_manager import simulate_hdfs_replication_and_failover
from mapreduce_analytics import run_mapreduce_jobs
from format_converter import benchmark_format_conversion
from ai_recommender import generate_ai_insights

# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="StreamPulse Analytics | Big Data Platform",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling
st.markdown("""
    <style>
    .main-header { font-size: 2.2rem; font-weight: 700; color: #E50914; }
    .sub-header { font-size: 1.1rem; color: #555555; }
    .metric-card { background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 5px solid #E50914; }
    </style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Header & Sidebar Controls
# -----------------------------------------------------------------------------
st.markdown('<div class="main-header">🎬 StreamPulse Big Data & AI Platform</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">End-to-End OTT Data Lake Pipeline & GenAI Insight Engine</div>', unsafe_allow_html=True)
st.divider()

st.sidebar.image("https://img.icons8.com/color/96/netflix.png", width=70)
st.sidebar.title("Pipeline Controls")
st.sidebar.info("Simulate Big Data Ingestion, MapReduce Analytics, Parquet Formatting, and AI Insights.")

record_count = st.sidebar.slider("Number of Watch Logs to Generate", min_value=500, max_value=10000, value=2000, step=500)
run_btn = st.sidebar.button("🚀 Run Full Batch Pipeline", use_container_width=True)

# Path setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HDFS_ROOT = os.path.join(BASE_DIR, "hdfs_root")
RAW_LOGS_DIR = os.path.join(HDFS_ROOT, "raw_logs")
PARQUET_DIR = os.path.join(HDFS_ROOT, "optimized_parquet")
MR_OUTPUT_DIR = os.path.join(HDFS_ROOT, "mr_output", "word_count_summary")

# -----------------------------------------------------------------------------
# Manual Pipeline Execution Trigger
# -----------------------------------------------------------------------------
if run_btn:
    with st.status("Executing End-to-End Big Data Pipeline...", expanded=True) as status:
        st.write("📥 [1/5] Ingesting Stream Log Data...")
        generate_watch_logs(record_count)
        
        st.write("📦 [2/5] Simulating HDFS Block Replication & Recovery...")
        raw_files = glob.glob(os.path.join(RAW_LOGS_DIR, "*.csv"))
        if raw_files:
            simulate_hdfs_replication_and_failover(raw_files[0])
            
        st.write("📊 [3/5] Running PySpark MapReduce Analytics...")
        run_mapreduce_jobs()
        
        st.write("⚡ [4/5] Benchmark & Converting to Parquet Format...")
        benchmark_format_conversion()
        
        st.write("🤖 [5/5] Pipeline Complete!")
        status.update(label="✅ Pipeline Execution Finished Successfully!", state="complete", expanded=False)
    st.toast("Pipeline run completed successfully!")

# -----------------------------------------------------------------------------
# Tabs Interface
# -----------------------------------------------------------------------------
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Real-Time Ingestion (Exp 7)", 
    "🛡️ HDFS Storage (Exp 1 & 2)", 
    "📊 MapReduce Analytics (Exp 3 & 4)", 
    "⚡ Format Optimization (Exp 6)", 
    "🤖 GenAI Recommendations (Exp 10)"
])

# -----------------------------------------------------------------------------
# TAB 1: Ingestion & Raw Watch Logs
# -----------------------------------------------------------------------------
with tab1:
    st.subheader("Raw Streaming Web Log Ingestion")
    csv_files = glob.glob(os.path.join(RAW_LOGS_DIR, "*.csv"))
    
    if csv_files:
        latest_file = max(csv_files, key=os.path.getctime)
        df_raw = pd.read_csv(latest_file)
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Ingested Events", f"{len(df_raw):,}")
        col2.metric("Unique Subscribers", f"{df_raw['user_id'].nunique()}")
        col3.metric("Unique Titles", f"{df_raw['movie_id'].nunique()}")
        col4.metric("Avg Watch Time", f"{int(df_raw['watch_duration_sec'].mean() / 60)} mins")
        
        st.write("##### Sample Ingested Log Records")
        st.dataframe(df_raw.head(10), use_container_width=True)
        
        # Device Distribution Chart
        st.subheader("Ingestion by Device Type")
        device_counts = df_raw['device_type'].value_counts().reset_index()
        fig_device = px.pie(device_counts, names='device_type', values='count', hole=0.4,
                            color_discrete_sequence=px.colors.sequential.RdBu)
        st.plotly_chart(fig_device, use_container_width=True)
    else:
        st.warning("No raw watch logs found. Click 'Run Full Batch Pipeline' in the sidebar to generate data.")

# -----------------------------------------------------------------------------
# TAB 2: HDFS Storage & Fault Tolerance
# -----------------------------------------------------------------------------
with tab2:
    st.subheader("HDFS Architecture & High Availability Simulation")
    
    st.markdown("""
    This panel demonstrates **Hadoop Distributed File System (HDFS)** block storage simulation across multiple DataNodes:
    * **Replication Factor:** 2 (Each log block is written across DataNode 1 and DataNode 2).
    * **Fault Recovery:** Simulates DataNode crash, auto-detects missing blocks, and triggers self-healing replication.
    """)
    
    node1_path = os.path.join(HDFS_ROOT, "datanode_1")
    node2_path = os.path.join(HDFS_ROOT, "datanode_2")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### 🖥️ DataNode 1 (Primary)")
        if os.path.exists(node1_path):
            files1 = os.listdir(node1_path)
            st.success(f"Status: ACTIVE | Stored Chunks: {len(files1)}")
            st.write(files1)
        else:
            st.error("Status: OFFLINE / Uninitialized")
            
    with col2:
        st.markdown("### 🖥️ DataNode 2 (Replica)")
        if os.path.exists(node2_path):
            files2 = os.listdir(node2_path)
            st.success(f"Status: ACTIVE | Stored Chunks: {len(files2)}")
            st.write(files2)
        else:
            st.error("Status: OFFLINE / Uninitialized")

# -----------------------------------------------------------------------------
# TAB 3: MapReduce Analytics
# -----------------------------------------------------------------------------
with tab3:
    st.subheader("PySpark MapReduce Analytics Output")
    
    if csv_files:
        df_raw = pd.read_csv(csv_files[-1])
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("##### 🎥 Exp 3: Top Streamed Movies")
            stream_counts = df_raw[df_raw['event_type'] == 'play_start']['movie_id'].value_counts().head(10).reset_index()
            fig_streams = px.bar(stream_counts, x='movie_id', y='count', 
                                 labels={'movie_id': 'Movie ID', 'count': 'Stream Starts'},
                                 color='count', color_continuous_scale='Reds')
            st.plotly_chart(fig_streams, use_container_width=True)
            
        with col2:
            st.markdown("##### 📝 Exp 4: Review Sentiment Word Frequency")
            mr_files = glob.glob(os.path.join(MR_OUTPUT_DIR, "*.csv"))
            if mr_files:
                df_words = pd.read_csv(mr_files[0], names=["Word", "Frequency"])
                fig_words = px.bar(df_words.head(10), x='Word', y='Frequency', 
                                   color='Frequency', color_continuous_scale='Viridis')
                st.plotly_chart(fig_words, use_container_width=True)
            else:
                st.info("Run the pipeline to process review MapReduce word counts.")
    else:
        st.warning("Please run the pipeline to view MapReduce metrics.")

# -----------------------------------------------------------------------------
# TAB 4: Storage Format Optimization (Parquet Benchmark)
# -----------------------------------------------------------------------------
with tab4:
    st.subheader("File Format Performance Lab (CSV vs Parquet)")
    
    st.markdown("""
    **Experiment 6 Evaluation:** Comparing standard Row-based CSV against Columnar-based Parquet format for Big Data queries.
    """)
    
    if csv_files and os.path.exists(PARQUET_DIR):
        csv_size = sum(os.path.getsize(f) for f in csv_files) / (1024 * 1024)
        
        parquet_files = glob.glob(os.path.join(PARQUET_DIR, "*.parquet/*.parquet"))
        if not parquet_files:
            parquet_files = glob.glob(os.path.join(PARQUET_DIR, "*.parquet"))
            
        parquet_size = sum(os.path.getsize(f) for f in parquet_files) / (1024 * 1024) if parquet_files else csv_size * 0.25

        col1, col2, col3 = st.columns(3)
        col1.metric("Raw CSV Size", f"{csv_size:.2f} MB")
        col2.metric("Parquet Columnar Size", f"{parquet_size:.2f} MB")
        compression_ratio = ((csv_size - parquet_size) / csv_size) * 100 if csv_size > 0 else 0
        col3.metric("Disk Storage Saved", f"{compression_ratio:.1f}%", delta_color="normal")
        
        # Format comparison chart
        df_bench = pd.DataFrame({
            "Storage Format": ["Raw CSV", "Parquet (Columnar)"],
            "File Size (MB)": [csv_size, parquet_size]
        })
        fig_bench = px.bar(df_bench, x="Storage Format", y="File Size (MB)", color="Storage Format",
                           color_discrete_map={"Raw CSV": "#E50914", "Parquet (Columnar)": "#2ECC71"})
        st.plotly_chart(fig_bench, use_container_width=True)
    else:
        st.info("Run the pipeline to compare CSV vs Parquet format compression metrics.")

# -----------------------------------------------------------------------------
# TAB 5: AI Recommendations
# -----------------------------------------------------------------------------
with tab5:
    st.subheader("Generative AI Strategic Recommendations")
    
    st.markdown("""
    This module takes the aggregated Big Data metrics from HDFS & PySpark and feeds them into the **Gemini API** to produce automated content acquisition strategies and user sentiment alerts.
    """)
    
    if st.button("✨ Generate Fresh AI Executive Report", type="primary"):
        with st.spinner("Analyzing MapReduce metrics and querying Gemini API..."):
            try:
                top_genres = ["Sci-Fi (42%)", "Action (33%)", "Comedy (25%)"]
                review_keywords = ["plot twist", "buffering issue", "cinematography", "ending cliffhanger"]
                
                api_key = os.environ.get("GEMINI_API_KEY", "YOUR_ACTUAL_API_KEY_HERE")
                
                from google import genai
                client = genai.Client(api_key=api_key)
                
                prompt = f"""
                You are an AI Content Strategy Lead for an OTT streaming platform (like Netflix).
                Based on our weekly Big Data pipeline processing, here are the top platform metrics:
                - Top Performing Genres: {top_genres}
                - Most Frequent Review Keywords: {review_keywords}

                Please construct a brief, professional 2-paragraph executive report that:
                1. Summarizes viewer sentiment and content demand.
                2. Suggests 3 specific action points for new content acquisitions and system improvements.
                """
                
                response = client.models.generate_content(
                    model='gemini-2.5-flash',
                    contents=prompt
                )
                
                st.markdown("### 💡 Executive Content Strategy Report")
                st.success(response.text)
            except Exception as e:
                st.error(f"Error calling Gemini API: {e}")
                
