# src/dq_checks/validity.py
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def check_range(df: DataFrame, column: str, min_value=None, max_value=None, threshold: float = 0.0) -> dict:
    total_count = df.count()
    if total_count == 0:
        return {"status": "PASS", "metric": 0.0, "message": f"Column '{column}' range check OK (empty dataframe).", "num_outside_range": 0, "total_rows": 0}
    conditions = []
    if min_value is not None:
        conditions.append(F.col(column) < min_value)
    if max_value is not None:
        conditions.append(F.col(column) > max_value)
    if not conditions:
         return {"status": "PASS", "metric": 0.0, "message": f"No range specified for column '{column}'.", "num_outside_range": 0, "total_rows": 0}
    filter_condition = conditions[0]
    for cond in conditions[1:]:
        filter_condition = filter_condition | cond
    outside_range_count = df.filter(F.col(column).isNotNull() & filter_condition).count()
    outside_range_ratio = outside_range_count / total_count if total_count > 0 else 0.0
    status = "PASS" if outside_range_ratio <= threshold else "FAIL"
    range_str = f"{f'min={min_value}' if min_value is not None else ''}{',' if min_value is not None and max_value is not None else ''}{f'max={max_value}' if max_value is not None else ''}"
    message = f"Column '{column}' has {outside_range_count} ({outside_range_ratio:.2%}) values outside range [{range_str}]. Threshold: {threshold:.2%}"
    return {
        "status": status, "metric": outside_range_ratio, "message": message,
        "num_outside_range": outside_range_count, "total_rows": total_count
    }

def check_regex(df: DataFrame, column: str, pattern: str, threshold: float = 0.0) -> dict:
    total_count = df.count()
    if total_count == 0:
        return {"status": "PASS", "metric": 0.0, "message": f"Column '{column}' regex check OK (empty dataframe).", "num_non_matching": 0, "total_rows": 0}
    non_matching_count = df.filter(F.col(column).isNotNull() & ~F.col(column).rlike(pattern)).count()
    non_matching_ratio = non_matching_count / total_count if total_count > 0 else 0.0
    status = "PASS" if non_matching_ratio <= threshold else "FAIL"
    message = f"Column '{column}' has {non_matching_count} ({non_matching_ratio:.2%}) values not matching regex '{pattern}'. Threshold: {threshold:.2%}"
    return {
        "status": status, "metric": non_matching_ratio, "message": message,
        "num_non_matching": non_matching_count, "total_rows": total_count
    }

def check_allowed_values(df: DataFrame, column: str, allowed_values: list, threshold: float = 0.0) -> dict:
    total_count = df.count()
    if total_count == 0:
        return {"status": "PASS", "metric": 0.0, "message": f"Column '{column}' allowed values check OK (empty dataframe).", "num_disallowed": 0, "total_rows": 0}
    disallowed_count = df.filter(F.col(column).isNotNull() & ~F.col(column).isin(allowed_values)).count()
    disallowed_ratio = disallowed_count / total_count if total_count > 0 else 0.0
    status = "PASS" if disallowed_ratio <= threshold else "FAIL"
    message = f"Column '{column}' has {disallowed_count} ({disallowed_ratio:.2%}) values not in {allowed_values}. Threshold: {threshold:.2%}"
    return {
        "status": status, "metric": disallowed_ratio, "message": message,
        "num_disallowed": disallowed_count, "total_rows": total_count
    }
