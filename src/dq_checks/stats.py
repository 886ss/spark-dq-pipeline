# 统计检查：Z-score均值偏离检测——过滤NaN后计算pop stddev，|实际均值-期望均值|/标准差 ≤ 允许偏差
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import NumericType
import math

def check_mean_deviation(df: DataFrame, column: str, expected_mean: float, max_deviation_stddevs: float, threshold: float = 0.0) -> dict:
    total_count = df.count()
    if total_count == 0:
        return {"status": "PASS", "metric": 0.0, "message": f"Mean deviation check OK on '{column}' (empty dataframe).", "actual_mean": None, "actual_stddev": None, "deviation_metric": 0.0}
    try:
        if not isinstance(df.schema[column].dataType, NumericType):
            raise TypeError(f"Column '{column}' is not numeric.")
    except (KeyError, TypeError) as e:
        return {"status": "ERROR", "metric": 1.0, "message": f"Mean deviation check failed: {e}", "actual_mean": None, "actual_stddev": None, "deviation_metric": 1.0}
    # Exclude NaN values: Spark treats NaN != NULL, so F.mean() includes NaN → NaN result
    clean_df = df.filter(F.col(column).isNotNull() & ~F.isnan(F.col(column)))
    stats = clean_df.select(
        F.mean(F.col(column)).alias("mean"),
        F.stddev_pop(F.col(column)).alias("stddev")
    ).first()
    actual_mean = stats["mean"]
    actual_stddev = stats["stddev"]
    # Use pandas isnull to handle both None and numpy NaN
    import numpy as np
    if actual_mean is None or actual_stddev is None or (isinstance(actual_mean, float) and not np.isfinite(actual_mean)) or (isinstance(actual_stddev, float) and not np.isfinite(actual_stddev)) or actual_stddev == 0:
         if actual_stddev == 0 and actual_mean != expected_mean:
              deviation_metric = float('inf')
              message = f"Mean deviation check on '{column}': Actual mean={actual_mean:.4f}, StdDev=0. Expected mean={expected_mean:.4f}. Deviation is infinite."
         elif actual_mean is None or math.isnan(actual_mean):
              deviation_metric = 1.0
              message = f"Mean deviation check on '{column}': Could not calculate valid stats (mean={actual_mean}, stddev={actual_stddev})."
              return {"status": "ERROR", "metric": deviation_metric, "message": message, "actual_mean": actual_mean, "actual_stddev": actual_stddev, "deviation_metric": deviation_metric}
         else:
              deviation_metric = 0.0
              message = f"Mean deviation check on '{column}': Actual mean={actual_mean:.4f}, StdDev={actual_stddev:.4f}. Expected mean={expected_mean:.4f}. Deviation=0.0 stddevs."
    else:
        deviation_metric = abs(actual_mean - expected_mean) / actual_stddev
        message = f"Mean deviation check on '{column}': Actual mean={actual_mean:.4f}, StdDev={actual_stddev:.4f}. Expected mean={expected_mean:.4f}. Deviation={deviation_metric:.4f} stddevs (Max allowed: {max_deviation_stddevs:.4f})."
    status = "FAIL" if deviation_metric > max_deviation_stddevs else "PASS"
    message += f" Configured check threshold (on metric): {threshold:.4f}"
    return {
        "status": status, "metric": deviation_metric, "message": message,
        "actual_mean": actual_mean, "actual_stddev": actual_stddev,
        "expected_mean": expected_mean, "max_deviation_stddevs": max_deviation_stddevs
    }
