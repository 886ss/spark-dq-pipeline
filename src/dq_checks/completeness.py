# src/dq_checks/completeness.py
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def check_not_null(df: DataFrame, column: str, threshold: float = 0.0) -> dict:
    total_count = df.count()
    if total_count == 0:
         return { "status": "PASS", "metric": 0.0, "message": f"Column '{column}' is OK (empty dataframe).", "num_nulls": 0, "total_rows": 0 }
    null_count = df.filter(F.col(column).isNull() | F.isnan(column)).count()
    null_ratio = null_count / total_count
    status = "PASS" if null_ratio <= threshold else "FAIL"
    message = f"Column '{column}' has {null_count} ({null_ratio:.2%}) null/NaN values. Threshold: {threshold:.2%}"
    return {
        "status": status,
        "metric": null_ratio,
        "message": message,
        "num_nulls": null_count,
        "total_rows": total_count
    }
