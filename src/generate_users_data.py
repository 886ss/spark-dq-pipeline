# 用户参考数据生成器：98.5万行用户记录(user_id/姓名/注册日期)，故意移除2% user_id用于验证参照完整性检查
import os
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import random
import time
import traceback
from datetime import datetime, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "reference")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "users.parquet")

os.makedirs(OUTPUT_DIR, exist_ok=True)
TOTAL_MAIN_USERS = 1_000_000
NUM_USERS_TO_GENERATE = int(TOTAL_MAIN_USERS * 0.98)
NUM_EXTRA_USERS = 5000

def generate_users(num_users, num_extra):
    print(f"Generating {num_users + num_extra} user records...")
    user_ids_main_subset = np.random.choice(range(1, TOTAL_MAIN_USERS + 1), num_users, replace=False)
    user_ids_extra = range(TOTAL_MAIN_USERS + 1, TOTAL_MAIN_USERS + 1 + num_extra)
    all_user_ids = np.concatenate((user_ids_main_subset, user_ids_extra))
    np.random.shuffle(all_user_ids)
    data = {
        'user_id': all_user_ids,
        'user_name': [f"user_{uid}" for uid in all_user_ids],
        'signup_date': [datetime(2021, 1, 1) + timedelta(days=random.randint(0, 365*2)) for _ in all_user_ids]
    }
    df = pd.DataFrame(data)
    print(f"Generated {len(df)} user records.")
    return df

if __name__ == "__main__":
    print("Starting users data generation...")
    start_gen_time = time.time()
    pdf = generate_users(NUM_USERS_TO_GENERATE, NUM_EXTRA_USERS)
    end_gen_time = time.time()
    print(f"Pandas DataFrame generation took {end_gen_time - start_gen_time:.2f} seconds.")
    print("Initializing Spark Session to write Users Parquet...")
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("UsersDataGenerator") \
            .master("local[*]") \
            .config("spark.driver.memory", "1g") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        print("Converting Users Pandas DataFrame to Spark DataFrame...")
        sdf = spark.createDataFrame(pdf)
        print(f"Writing users data to {OUTPUT_DIR}...")
        sdf.repartition(1).write.mode("overwrite").parquet(OUTPUT_DIR)
        try:
            part_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith('part-') and f.endswith('.parquet')]
            if not part_files: raise FileNotFoundError("No Parquet part file found.")
            part_file = part_files[0]
            if os.path.exists(OUTPUT_FILE): os.remove(OUTPUT_FILE)
            os.replace(os.path.join(OUTPUT_DIR, part_file), OUTPUT_FILE)
            print(f"Successfully renamed {part_file} to {os.path.basename(OUTPUT_FILE)}")
            success_file = os.path.join(OUTPUT_DIR, "_SUCCESS")
            if os.path.exists(success_file): os.remove(success_file)
            for f in os.listdir(OUTPUT_DIR):
               if f.endswith(".crc") or (f.startswith("part-") and f.endswith(".parquet")):
                   try: os.remove(os.path.join(OUTPUT_DIR, f))
                   except OSError: pass
        except Exception as e:
            print(f"An error occurred during file renaming/cleanup: {e}")
            traceback.print_exc()
    finally:
        if spark: spark.stop()
    print("\nUsers data generation complete.")
    print(f"File saved to: {OUTPUT_FILE}")
