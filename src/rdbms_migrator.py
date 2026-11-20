import subprocess
import os
from pyspark.sql import SparkSession

def get_wsl_host_ip():
    """Dynamically finds the Windows host IP address from inside WSL."""
    try:
        result = subprocess.run(["ip", "route"], capture_output=True, text=True)
        for line in result.stdout.splitlines():
            if "default via" in line:
                return line.split()[2]
    except Exception:
        pass
    return "127.0.0.1"

def migrate_mysql_to_hdfs(mysql_user="root", mysql_password="nipnip18"):
    spark = SparkSession.builder \
        .appName("OTT_MySQL_Migrator") \
        .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.3.0") \
        .master("local[*]") \
        .getOrCreate()

    host_ip = get_wsl_host_ip()
    jdbc_url = f"jdbc:mysql://{host_ip}:3306/ott_platform?allowPublicKeyRetrieval=true&useSSL=false"
    
    db_properties = {
        "user": mysql_user,
        "password": mysql_password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir = os.path.join(BASE_DIR, "hdfs_root", "rdbms_exports", "movies_catalog")
    
    print(f"\n📥 Fetching 'movies' table from MySQL at {host_ip}:3306...")
    try:
        movies_df = spark.read.jdbc(url=jdbc_url, table="movies", properties=db_properties)
        movies_df.write.mode("overwrite").parquet(f"file:///{output_dir}")
        print(f"✅ MySQL Data Migrated successfully to HDFS -> {output_dir}")
    except Exception as e:
        print(f"❌ Migration Error: {e}")

    spark.stop()

if __name__ == "__main__":
    migrate_mysql_to_hdfs()
