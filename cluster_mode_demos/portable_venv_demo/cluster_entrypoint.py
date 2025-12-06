import os
from pathlib import Path
import sys


if __name__ == "__main__":
    print("=" * 80)
    print("CLUSTER ENTRYPOINT STARTED")
    print("=" * 80)
    
    print(f"Current working dir: {os.getcwd()}")
    print(f"sys.path: {sys.path}")
    print(f"Python version: {sys.version}")
    print(f"Python executable path: {sys.executable}")
    
    print("\nDirectory contents:")
    for item in os.listdir('.'):
        print(f"  - {item}")
    
    print(f"\netl_repo exists: {Path('etl_repo').exists()}")
    print(f"etl_repo.zip exists: {Path('etl_repo.zip').exists()}")
    
    if Path('etl_repo').exists():
        print(f"etl_repo contents: {os.listdir('etl_repo')}")
        sys.path.insert(0, str(Path('etl_repo').resolve()))
    
    print("\n" + "=" * 80)
    print("CREATING SPARK SESSION")
    print("=" * 80)
    
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("Portable Venv Demo") \
        .getOrCreate()
    
    print(f"Spark version: {spark.version}")
    print(f"Spark master: {spark.sparkContext.master}")
    print(f"App ID: {spark.sparkContext.applicationId}")
    
    print("\n" + "=" * 80)
    print("TESTING BASIC SPARK OPERATIONS")
    print("=" * 80)
    
    data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    df = spark.createDataFrame(data, ["id", "name"])
    
    print("\nCreated DataFrame:")
    df.show()
    
    print(f"Row count: {df.count()}")
    
    print("\n" + "=" * 80)
    print("CLUSTER ENTRYPOINT FINISHED SUCCESSFULLY")
    print("=" * 80)
    
    spark.stop()