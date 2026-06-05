# src/data_generator.py
import os
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import random
import time
import traceback

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "synthetic_errors")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "synthetic_data.parquet")

os.makedirs(OUTPUT_DIR, exist_ok=True)
NUM_ROWS = 1_000_000

def generate_data(num_rows):
    print(f"Generating {num_rows} rows of synthetic data with corrected date logic...")
    data = {
        'user_id': range(1, num_rows + 1),
        'transaction_id': [f"txn_{i}_{random.randint(1000, 9999)}" for i in range(num_rows)],
        'product_id': [f"P{random.randint(100, 199)}" for _ in range(num_rows)],
        'price': np.random.uniform(5.0, 500.0, num_rows).round(2),
        'quantity': np.random.randint(1, 10, num_rows).astype(float),
        'email': [f"user{i}@example.com" for i in range(num_rows)],
        'country_code': [random.choice(['US', 'CA', 'MX', 'GB', 'DE', 'FR', 'INVALID']) for _ in range(num_rows)],
        'order_status': [random.choice(['COMPLETED', 'PENDING', 'CANCELLED', 'FAILED', 'UNK']) for _ in range(num_rows)],
    }
    buffer_seconds = 60 * 60 * 24 * 60
    start_dates_unix = np.random.randint(1640995200, 1672531199 - buffer_seconds, num_rows)
    data['start_date'] = pd.to_datetime(start_dates_unix, unit='s')
    min_duration_seconds = 3600
    max_duration_seconds = buffer_seconds
    durations_seconds = np.random.randint(min_duration_seconds, max_duration_seconds + 1, num_rows)
    data['end_date'] = pd.to_datetime(start_dates_unix + durations_seconds, unit='s')
    df = pd.DataFrame(data)

    null_indices_price = df.sample(frac=0.05).index
    df.loc[null_indices_price, 'price'] = np.nan
    null_indices_qty = df.sample(frac=0.05).index
    df.loc[null_indices_qty, 'quantity'] = np.nan
    range_error_indices = df.sample(frac=0.01).index
    df.loc[range_error_indices, 'price'] = df.loc[range_error_indices, 'price'] * -1
    regex_error_indices = df.sample(frac=0.02).index
    df.loc[regex_error_indices, 'email'] = [f"user{i}_invalid-format" for i in regex_error_indices]
    num_duplicates_to_create = int(num_rows * 0.03)
    if num_duplicates_to_create > 0:
        dup_source_indices = df.sample(n=num_duplicates_to_create).index
        potential_target_indices = df.index.difference(dup_source_indices)
        if len(potential_target_indices) >= num_duplicates_to_create:
             dup_target_indices = np.random.choice(potential_target_indices, num_duplicates_to_create, replace=False)
             df.loc[dup_target_indices, 'transaction_id'] = df.loc[dup_source_indices, 'transaction_id'].values
        else:
             print(f"Warning: Could not create all {num_duplicates_to_create} unique duplicates due to dataset size.")
    consistency_error_indices = df.sample(frac=0.10).index
    temp_start = df.loc[consistency_error_indices, 'start_date'].copy()
    df.loc[consistency_error_indices, 'start_date'] = df.loc[consistency_error_indices, 'end_date']
    df.loc[consistency_error_indices, 'end_date'] = temp_start
    return df

if __name__ == "__main__":
    print(f"Starting data generation for {NUM_ROWS} rows...")
    start_gen_time = time.time()
    pdf = generate_data(NUM_ROWS)
    end_gen_time = time.time()
    print(f"Pandas DataFrame generation took {end_gen_time - start_gen_time:.2f} seconds.")

    print("Initializing Spark Session to write Parquet...")
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("DataGenerator") \
            .master("local[*]") \
            .config("spark.sql.parquet.writeLegacyFormat", "true") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        print("Converting Pandas DataFrame to Spark DataFrame...")
        start_convert_time = time.time()
        sdf = spark.createDataFrame(pdf)
        end_convert_time = time.time()
        print(f"Spark DataFrame conversion took {end_convert_time - start_convert_time:.2f} seconds.")
        print(f"Writing data to {OUTPUT_DIR}...")
        start_write_time = time.time()
        sdf.repartition(1).write.mode("overwrite").parquet(OUTPUT_DIR)
        end_write_time = time.time()
        print(f"Spark Parquet writing took {end_write_time - start_write_time:.2f} seconds.")
        try:
            part_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith('part-') and f.endswith('.parquet')]
            if not part_files:
                 raise FileNotFoundError("No Parquet part file found in output directory.")
            part_file = part_files[0]
            if os.path.exists(OUTPUT_FILE):
                print(f"Removing existing target file: {OUTPUT_FILE}")
                os.remove(OUTPUT_FILE)
            os.replace(os.path.join(OUTPUT_DIR, part_file), OUTPUT_FILE)
            print(f"Successfully renamed {part_file} to {os.path.basename(OUTPUT_FILE)}")
            success_file = os.path.join(OUTPUT_DIR, "_SUCCESS")
            if os.path.exists(success_file):
                os.remove(success_file)
            for f in os.listdir(OUTPUT_DIR):
               if f.endswith(".crc") or (f.startswith("part-") and f.endswith(".parquet")):
                   try: os.remove(os.path.join(OUTPUT_DIR, f))
                   except OSError: pass
        except Exception as e:
            print(f"An error occurred during file renaming/cleanup: {e}")
            traceback.print_exc()
    except Exception as e:
         print(f"A critical error occurred during Spark operation: {e}")
         traceback.print_exc()
    finally:
        if spark:
            print("Stopping Spark Session.")
            spark.stop()
    print("\nSynthetic data generation complete.")
    print(f"File saved to: {OUTPUT_FILE}")
    print("\nSample of generated data (first 5 rows):")
    try:
        print(pdf.head().to_markdown(index=False, tablefmt="pipe"))
    except ImportError:
        print("(Install 'tabulate' with pip to see sample data formatted as Markdown table)")
        print(pdf.head())
