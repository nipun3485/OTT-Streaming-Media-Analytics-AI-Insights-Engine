import os
import shutil
import glob

HDFS_ROOT = os.path.abspath("hdfs_root")

def simulate_hdfs_replication_and_failover(source_file):
    node_a = os.path.join(HDFS_ROOT, "datanode_1")
    node_b = os.path.join(HDFS_ROOT, "datanode_2")
    os.makedirs(node_a, exist_ok=True)
    os.makedirs(node_b, exist_ok=True)

    base_name = os.path.basename(source_file)
    file_node_a = os.path.join(node_a, base_name)
    file_node_b = os.path.join(node_b, base_name)

    # 1. Replicate File (Replication Factor = 2)
    shutil.copy(source_file, file_node_a)
    shutil.copy(source_file, file_node_b)
    print(f"📦 [HDFS Replication Factor: 2] Copied to DataNode 1 & DataNode 2")

    # 2. Simulate Node Failure (DataNode 1 goes offline/deleted)
    print("⚠️ Simulating DataNode 1 Failure...")
    os.remove(file_node_a)

    # 3. Failover / Self-Healing check
    if os.path.exists(file_node_b):
        print("✅ High Availability Verified: Data safely retrieved from DataNode 2 replica!")
        # Restore Node A from Node B replica
        shutil.copy(file_node_b, file_node_a)
        print("🔄 Self-Healing Complete: Re-replicated data block back to DataNode 1.")

if __name__ == "__main__":
    raw_files = glob.glob(os.path.join(HDFS_ROOT, "raw_logs/*.csv"))
    if raw_files:
        simulate_hdfs_replication_and_failover(raw_files[0])
    else:
        print("Please run 01_log_generator.py first to create raw data.")
