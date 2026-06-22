# 一致性检查：自定义SQL条件(F.expr) + 跨表参照完整性(left_anti join，等价NOT EXISTS，Spark专门优化)
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
import os

def check_custom_condition(df: DataFrame, condition_sql: str, threshold: float = 0.0) -> dict:
    total_count = df.count()
    if total_count == 0:
         return {"status": "PASS", "metric": 0.0, "message": f"Consistency check OK (empty dataframe). Condition: '{condition_sql}'", "num_failed_condition": 0, "total_rows": 0}
    try:
        failed_condition_count = df.filter(F.expr(f"NOT ({condition_sql})")).count()
    except Exception as e:
        return {"status": "ERROR", "metric": 1.0, "message": f"Error evaluating consistency condition '{condition_sql}': {e}", "num_failed_condition": -1, "total_rows": total_count}
    failed_ratio = failed_condition_count / total_count if total_count > 0 else 0.0
    status = "PASS" if failed_ratio <= threshold else "FAIL"
    message = f"Consistency check failed for {failed_condition_count} ({failed_ratio:.2%}) rows. Condition: '{condition_sql}'. Threshold: {threshold:.2%}"
    return {
        "status": status, "metric": failed_ratio, "message": message,
        "num_failed_condition": failed_condition_count, "total_rows": total_count
    }

def check_referential_integrity(df: DataFrame, column: str, reference_data_path: str, reference_column: str, threshold: float = 0.0) -> dict:
    total_count = df.count()
    spark = SparkSession.getActiveSession()
    if total_count == 0 or not spark:
        status = "PASS" if total_count==0 else "ERROR"
        message = "Referential integrity check OK (empty main dataframe)." if total_count==0 else "No active Spark session found."
        return {"status": status, "metric": 0.0, "message": message, "num_missing_refs": 0, "total_rows": total_count}
    abs_ref_path = reference_data_path
    if not os.path.isabs(reference_data_path):
         project_root = "/app"
         abs_ref_path = os.path.join(project_root, reference_data_path)
    print(f"Attempting to load reference data from: {abs_ref_path}")
    try:
        if not os.path.exists(abs_ref_path):
             raise FileNotFoundError(f"Reference data path not found inside container: {abs_ref_path}")
        ref_df = spark.read.parquet(abs_ref_path)
        ref_keys = ref_df.select(F.col(reference_column).alias(f"ref_{reference_column}")).distinct()
        missing_refs_df = df.join(
            ref_keys,
            df[column] == ref_keys[f"ref_{reference_column}"],
            how='left_anti'
        )
        missing_refs_count = missing_refs_df.count()
    except FileNotFoundError as e:
         return {"status": "ERROR", "metric": 1.0, "message": f"Referential integrity check failed: {e}", "num_missing_refs": -1, "total_rows": total_count}
    except Exception as e:
         import traceback
         tb_str = traceback.format_exc()
         return {"status": "ERROR", "metric": 1.0, "message": f"Error during referential integrity check on '{column}' vs '{reference_column}': {e}\n{tb_str}", "num_missing_refs": -1, "total_rows": total_count}
    missing_ratio = missing_refs_count / total_count if total_count > 0 else 0.0
    status = "PASS" if missing_ratio <= threshold else "FAIL"
    message = f"Referential integrity check: {missing_refs_count} ({missing_ratio:.2%}) rows in column '{column}' have values not found in reference column '{reference_column}' from '{reference_data_path}'. Threshold: {threshold:.2%}"
    return {
        "status": status, "metric": missing_ratio, "message": message,
        "num_missing_refs": missing_refs_count, "total_rows": total_count
    }
