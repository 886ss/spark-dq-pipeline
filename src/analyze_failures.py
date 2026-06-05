# src/analyze_failures.py
import os
import sys
import traceback
from pyspark.sql import functions as F

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

try:
    from utils.spark_utils import get_spark_session, stop_spark_session
except ImportError:
    print("Error: Could not import Spark utilities.")
    sys.exit(1)

DATA_DIR = os.path.join(PROJECT_ROOT, "data", "synthetic_errors")
INITIAL_DATA_PATH = os.path.join(DATA_DIR)
SINGLE_FILE_NAME = "synthetic_data.parquet"


def analyze_price_range_failures():
    spark = None
    data_load_path = INITIAL_DATA_PATH
    try:
        spark = get_spark_session("DQFailureAnalysisApp")
        print(f"Checking for Parquet data. Initial path: {data_load_path}")
        if not os.path.isdir(data_load_path):
             print(f"Directory '{data_load_path}' not found or not a directory.")
             single_file_path = os.path.join(DATA_DIR, SINGLE_FILE_NAME)
             if os.path.isfile(single_file_path):
                  print(f"Found single file: {single_file_path}. Using it for loading.")
                  data_load_path = single_file_path
             else:
                  raise FileNotFoundError(f"Data directory '{INITIAL_DATA_PATH}' or single file '{single_file_path}' not found inside the container.")
        else:
            print(f"Using directory '{data_load_path}' for loading.")
        df = spark.read.parquet(data_load_path)
        print("\nData loaded successfully. Schema:")
        df.printSchema()
        initial_count = df.count()
        print(f"Total rows loaded: {initial_count}")
        if initial_count == 0:
             print("Warning: Loaded DataFrame is empty.")
             return
        print("\n--- Investigating Price Range Failures (price < 0.0) ---")
        failed_price_df = df.filter(F.col("price").isNotNull() & (F.col("price") < 0.0))
        failure_count = failed_price_df.count()
        print(f"\nFound {failure_count} rows with negative prices (price < 0.0).")
        if failure_count > 0:
            print("\nSample of failing rows (showing up to 20 rows):")
            failed_price_df.show(20, truncate=False)
        else:
            print("\nNo rows found failing the price range check (price < 0.0).")
    except FileNotFoundError as fnf_error:
         print(f"\nERROR: {fnf_error}")
    except Exception as e:
        print(f"\nAn unexpected error occurred during the analysis: {e}")
        traceback.print_exc()
    finally:
        if spark:
            stop_spark_session(spark)
            print("\nSpark session stopped.")

if __name__ == "__main__":
    print("Starting Data Quality Failure Analysis Script...")
    analyze_price_range_failures()
    print("\nAnalysis script finished.")
