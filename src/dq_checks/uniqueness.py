# 唯一性检查：groupBy+count检测列中重复值占比，排除NULL后计算重复行比例
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def check_uniqueness(df: DataFrame, column: str, threshold: float = 0.0) -> dict:
    total_count = df.count()
    if total_count == 0:
         return { "status": "PASS", "metric": 0.0, "message": f"Column '{column}' is unique (empty dataframe).", "num_duplicates": 0, "total_rows": 0 }
    non_null_df = df.filter(F.col(column).isNotNull())
    total_non_null_count = non_null_df.count()
    if total_non_null_count == 0:
        return { "status": "PASS", "metric": 0.0, "message": f"Column '{column}' contains only NULL values.", "num_duplicates": 0, "total_rows": total_count }
    duplicates_count_df = non_null_df.groupBy(column).count()
    duplicate_values_df = duplicates_count_df.filter(F.col("count") > 1)
    if duplicate_values_df.count() == 0:
         return { "status": "PASS", "metric": 0.0, "message": f"Column '{column}' has all unique values.", "num_duplicates": 0, "total_rows": total_count }
    num_rows_with_duplicates = non_null_df.join(duplicate_values_df.select(column), on=column, how="inner").count()
    duplicate_ratio = num_rows_with_duplicates / total_count if total_count > 0 else 0.0
    status = "PASS" if duplicate_ratio <= threshold else "FAIL"
    message = f"Column '{column}' has {num_rows_with_duplicates} rows ({duplicate_ratio:.2%}) with non-unique values (excluding nulls). Threshold: {threshold:.2%}"
    return {
        "status": status,
        "metric": duplicate_ratio,
        "message": message,
        "num_duplicates": num_rows_with_duplicates,
        "total_rows": total_count
    }
