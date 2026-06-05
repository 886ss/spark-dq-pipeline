# src/main_pipeline.py
import os
import sys
import yaml
import json
import csv
from datetime import datetime
import time
import traceback
import uuid
import numpy as np

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

try:
    from utils.spark_utils import get_spark_session, stop_spark_session
except ImportError as ie:
     print(f"Error importing utility functions: {ie}")
     sys.exit(1)

try:
    from dq_checks import completeness, uniqueness, validity, consistency
    from dq_checks import format
    from dq_checks import stats
except ImportError as ie:
     print(f"Error importing DQ check functions: {ie}")
     sys.exit(1)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "dq_checks_config.yaml")
SINGLE_FILE_NAME = "synthetic_data.parquet"


def load_config(path):
    print(f"Loading configuration from: {path}")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Configuration file not found at {path}")
    try:
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
        print("Configuration loaded successfully.")
        return config
    except Exception as e:
        print(f"Error loading configuration: {e}")
        raise

def get_check_function(check_type, sub_type=None):
    if check_type == "completeness":
        return completeness.check_not_null
    elif check_type == "uniqueness":
        return uniqueness.check_uniqueness
    elif check_type == "validity":
        if sub_type == "range":
            return validity.check_range
        elif sub_type == "regex":
            return validity.check_regex
        elif sub_type == "categorical":
            return validity.check_allowed_values
        else:
             raise ValueError(f"Unknown validity sub_type: {sub_type}")
    elif check_type == "consistency":
        if sub_type == "referential_integrity":
            try:
                return consistency.check_referential_integrity
            except AttributeError:
                 raise ValueError("Function 'check_referential_integrity' not found in consistency module.")
        elif sub_type is None or sub_type == "custom_condition":
             try:
                return consistency.check_custom_condition
             except AttributeError:
                  raise ValueError("Function 'check_custom_condition' not found in consistency module.")
        else:
             raise ValueError(f"Unknown consistency sub_type: {sub_type}")
    elif check_type == "format":
        if sub_type == "string_length":
             return format.check_string_length
        elif sub_type == "date_format":
             return format.check_date_format
        elif sub_type == "data_type":
             return format.check_data_type
        else:
             raise ValueError(f"Unknown format sub_type: {sub_type}")
    elif check_type == "stats":
        if sub_type == "mean_deviation":
             return stats.check_mean_deviation
        else:
             raise ValueError(f"Unknown stats sub_type: {sub_type}")
    else:
        raise ValueError(f"Unknown check type: {check_type}")

def write_report(results, config):
    report_config = config.get('reporting', {})
    output_formats = report_config.get('output_formats', ['json'])
    report_dir = os.path.join(PROJECT_ROOT, report_config.get('report_dir', 'reports'))
    filename_prefix = report_config.get('report_filename_prefix', 'dq_report_docker')
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(report_dir, exist_ok=True)
    report_data = []
    for check_name, result in results.items():
        metric_val = result.get("metric")
        if isinstance(metric_val, float) and not np.isfinite(metric_val):
             metric_serializable = None
        else:
             metric_serializable = metric_val
        report_entry = {
            "check_name": check_name,
            "timestamp": result.get("run_timestamp"),
            "status": result.get("status"),
            "metric": metric_serializable,
            "message": result.get("message"),
            "parameters": json.dumps(result.get("parameters")),
            "details": json.dumps({k: v for k, v in result.items() if k not in ['status', 'metric', 'message', 'parameters', 'run_timestamp']})
        }
        report_data.append(report_entry)
    if not report_data:
        print("No results to write to report.")
        return
    if 'json' in output_formats:
        json_path = os.path.join(report_dir, f"{filename_prefix}_{timestamp}.json")
        try:
            with open(json_path, 'w') as f:
                json.dump(report_data, f, indent=4)
            print(f"JSON report written to: {json_path}")
        except Exception as e:
            print(f"Error writing JSON report: {e}")
    if 'csv' in output_formats:
        csv_path = os.path.join(report_dir, f"{filename_prefix}_{timestamp}.csv")
        try:
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                if report_data:
                    fieldnames = report_data[0].keys()
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(report_data)
            print(f"CSV report written to: {csv_path}")
        except Exception as e:
            print(f"Error writing CSV report: {e}")


def apply_data_cleaning(df: F.DataFrame) -> F.DataFrame:
    print("\n--- Applying Data Cleaning Steps ---")
    start_cleaning_time = time.time()
    valid_email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    allowed_countries = ["US", "CA", "MX", "GB", "DE", "FR"]
    allowed_statuses = ["COMPLETED", "PENDING", "CANCELLED", "FAILED"]

    df_basic_cleaned = df \
        .withColumn("price_original", F.col("price")) \
        .withColumn("price",
                    F.when(F.col("price").isNull(), F.lit(None).cast("double"))
                    .when(F.col("price") < 0.0, 0.0)
                    .otherwise(F.col("price"))) \
        .withColumn("email_original", F.col("email")) \
        .withColumn("email",
                    F.when(F.col("email").isNull(), F.lit(None).cast("string"))
                    .when(F.col("email").rlike(valid_email_pattern), F.col("email"))
                    .otherwise(F.lit(None).cast("string"))) \
        .withColumn("country_code_original", F.col("country_code")) \
        .withColumn("country_code",
                    F.when(F.col("country_code").isNull(), F.lit(None).cast("string"))
                    .when(F.col("country_code").isin(allowed_countries), F.col("country_code"))
                    .otherwise("UNKNOWN")) \
        .withColumn("order_status_original", F.col("order_status")) \
        .withColumn("order_status",
                    F.when(F.col("order_status").isNull(), F.lit(None).cast("string"))
                    .when(F.col("order_status").isin(allowed_statuses), F.col("order_status"))
                    .otherwise("UNKNOWN")) \
        .withColumn("dates_marked_inconsistent",
                    F.when(F.col("end_date").isNull() | F.col("start_date").isNull(), False)
                    .when(F.col("end_date") < F.col("start_date"), True)
                    .otherwise(False))

    print("Applying deduplication for transaction_id (keeping latest start_date)...")
    df_nulls = df_basic_cleaned.filter(F.col("transaction_id").isNull() | F.col("start_date").isNull())
    df_to_dedupe = df_basic_cleaned.filter(F.col("transaction_id").isNotNull() & F.col("start_date").isNotNull())
    count_to_dedupe = df_to_dedupe.count()
    count_nulls = df_nulls.count()
    print(f"Rows to process for deduplication: {count_to_dedupe}")
    print(f"Rows with null transaction_id or start_date (kept separately): {count_nulls}")

    if count_to_dedupe > 0:
        window_spec = Window.partitionBy("transaction_id").orderBy(F.col("start_date").desc())
        df_deduplicated = df_to_dedupe.withColumn("row_num", F.row_number().over(window_spec)) \
                                      .filter(F.col("row_num") == 1) \
                                      .drop("row_num")
        count_after_dedupe = df_deduplicated.count()
        print(f"Rows remaining after deduplication: {count_after_dedupe}")
        df_cleaned = df_deduplicated.unionByName(df_nulls, allowMissingColumns=True)
        final_count = df_cleaned.count()
        print(f"Total rows after combining deduplicated and null-key rows: {final_count}")
    else:
        print("No rows found with non-null transaction_id and start_date for deduplication.")
        df_cleaned = df_nulls

    end_cleaning_time = time.time()
    print(f"Data cleaning steps took {end_cleaning_time - start_cleaning_time:.2f} seconds.")
    print("\nSchema after all cleaning steps:")
    df_cleaned.printSchema()
    print("Sample data after all cleaning steps:")
    df_cleaned.show(5, truncate=False)
    return df_cleaned


def run_pipeline():
    spark = None
    df_raw = None
    df_to_check = None
    results = {}
    start_time = time.time()
    data_source_path = "N/A"

    try:
        config = load_config(CONFIG_PATH)
        data_source_relative = config.get('data_source')
        if not data_source_relative:
            raise ValueError("Missing 'data_source' in configuration.")
        data_source_path = os.path.abspath(os.path.join(PROJECT_ROOT, data_source_relative))
        print(f"Resolved data source path: {data_source_path}")

        spark = get_spark_session("DataQualityPipeline")

        print(f"\nLoading raw data from: {data_source_path}")
        if not os.path.exists(data_source_path):
             alt_path = os.path.join(os.path.dirname(data_source_path), SINGLE_FILE_NAME)
             print(f"Path {data_source_path} not found. Trying single file: {alt_path}")
             if os.path.isfile(alt_path):
                  data_source_path = alt_path
             else:
                  raise FileNotFoundError(f"Data source directory or file not found at resolved path: {data_source_path} or {alt_path}")

        df_raw = spark.read.parquet(data_source_path)
        print("\nRaw data loaded successfully.")

        df_cleaned = apply_data_cleaning(df_raw)
        df_to_check = df_cleaned.cache()
        print("\n--- Data Quality Checks will run on CLEANED & DEDUPLICATED data ---")

        checks_to_run = config.get('checks', [])
        print(f"\n--- Running {len(checks_to_run)} Data Quality Checks ---")

        for check_config in checks_to_run:
            check_name = check_config.get('check_name')
            check_type = check_config.get('check_type')
            sub_type = check_config.get('sub_type')
            params = check_config.get('params', {})
            run_timestamp = datetime.now().isoformat()

            if not check_name or not check_type:
                print(f"Skipping invalid check config: {check_config}")
                continue

            print(f"\nExecuting Check: {check_name} (Type: {check_type}{f'/{sub_type}' if sub_type else ''})")
            print(f"Parameters: {params}")

            try:
                check_function = get_check_function(check_type, sub_type)
                result = check_function(df_to_check, **params)
                result['run_timestamp'] = run_timestamp
                result['parameters'] = params
                results[check_name] = result
                print(f"Result: Status={result['status']}, Metric={result.get('metric', 'N/A')}")
                print(f"Message: {result['message']}")
            except Exception as e:
                print(f"ERROR executing check '{check_name}': {e}")
                traceback.print_exc()
                results[check_name] = {
                    "status": "ERROR", "message": f"Failed to execute check: {str(e)}",
                    "run_timestamp": run_timestamp, "parameters": params, "metric": None
                }

        print("\n--- All Checks Completed ---")
        print("\n--- Generating Report ---")
        write_report(results, config)

        print("\n--- Checking Database Logging Configuration ---")
        db_logging_config = config.get('database_logging', {})
        if db_logging_config.get('enabled', False):
            print("Database logging is ENABLED in config. Attempting to log...")
            try:
                from utils.db_utils import log_results_to_postgres
                db_conn_str = os.environ.get("DB_CONNECTION_STRING")
                if not db_conn_str:
                    print("DB_CONNECTION_STRING environment variable not found. Checking config file...")
                    db_conn_str = db_logging_config.get("connection_string")
                if db_conn_str:
                     print("Found database connection string. Proceeding with logging.")
                     run_id = str(uuid.uuid4())
                     db_config_for_log = {
                         "connection_string": db_conn_str,
                         "table_name": db_logging_config.get("table_name", "dq_results_log")
                     }
                     log_results_to_postgres(results, db_config_for_log, pipeline_run_id=run_id, data_source=data_source_path)
                else:
                     print("WARNING: Database logging enabled but no connection string found.")
            except ImportError:
                 print("ERROR: Database logging enabled, but 'utils.db_utils.log_results_to_postgres' could not be imported.")
                 traceback.print_exc()
            except Exception as db_err:
                 print(f"ERROR: An unexpected error occurred during database logging attempt: {db_err}")
                 traceback.print_exc()
        else:
             print("Database logging is DISABLED in the configuration file.")

    except FileNotFoundError as fnf_error:
         print(f"\nERROR: {fnf_error}")
         print(f"Attempted to access path: {data_source_path}")
    except Exception as e:
        print(f"\nAn unexpected error occurred during the pipeline execution: {e}")
        traceback.print_exc()
    finally:
        if 'df_raw' in locals() and df_raw is not None and df_raw.is_cached:
            print("Unpersisting raw DataFrame.")
            df_raw.unpersist()
        if 'df_to_check' in locals() and df_to_check is not None and df_to_check.is_cached:
            print("Unpersisting cleaned DataFrame.")
            df_to_check.unpersist()
        if spark:
            stop_spark_session(spark)
        end_time = time.time()
        print(f"\nPipeline finished in {end_time - start_time:.2f} seconds.")


if __name__ == "__main__":
    run_pipeline()
