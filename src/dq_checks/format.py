# 格式检查：字符串长度约束(fixed/min/max) + 日期格式解析(to_timestamp) + 列数据类型校验(schema[col].dataType)
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

def check_string_length(df: DataFrame, column: str, min_length: int = None, max_length: int = None, fixed_length: int = None, threshold: float = 0.0) -> dict:
    total_count = df.count()
    if total_count == 0:
        return {"status": "PASS", "metric": 0.0, "message": f"Column '{column}' length check OK (empty dataframe).", "num_invalid_length": 0, "total_rows": 0}
    if not isinstance(df.schema[column].dataType, StringType):
         return {"status": "ERROR", "metric": 1.0, "message": f"Column '{column}' is not StringType, cannot check length.", "num_invalid_length": -1, "total_rows": total_count}
    base_filter = F.col(column).isNotNull()
    length_col = F.length(F.col(column))
    conditions = []
    description = []
    if fixed_length is not None:
        conditions.append(length_col != fixed_length)
        description.append(f"fixed_length={fixed_length}")
    else:
        if min_length is not None:
            conditions.append(length_col < min_length)
            description.append(f"min_length={min_length}")
        if max_length is not None:
            conditions.append(length_col > max_length)
            description.append(f"max_length={max_length}")
    if not conditions:
        return {"status": "PASS", "metric": 0.0, "message": f"No length constraints specified for column '{column}'.", "num_invalid_length": 0, "total_rows": 0}
    filter_condition = conditions[0]
    for cond in conditions[1:]:
        filter_condition = filter_condition | cond
    invalid_length_count = df.filter(base_filter & filter_condition).count()
    invalid_ratio = invalid_length_count / total_count if total_count > 0 else 0.0
    status = "PASS" if invalid_ratio <= threshold else "FAIL"
    range_str = ", ".join(description)
    message = f"Column '{column}' has {invalid_length_count} ({invalid_ratio:.2%}) non-null values with length outside constraints [{range_str}]. Threshold: {threshold:.2%}"
    return {
        "status": status, "metric": invalid_ratio, "message": message,
        "num_invalid_length": invalid_length_count, "total_rows": total_count
    }

def check_date_format(df: DataFrame, column: str, expected_format: str, threshold: float = 0.0) -> dict:
    total_count = df.count()
    if total_count == 0:
        return {"status": "PASS", "metric": 0.0, "message": f"Column '{column}' date format check OK (empty dataframe).", "num_unparseable": 0, "total_rows": 0}
    if not isinstance(df.schema[column].dataType, StringType):
         return {"status": "ERROR", "metric": 1.0, "message": f"Column '{column}' is not StringType, cannot check date format.", "num_unparseable": -1, "total_rows": total_count}
    parsed_col = F.to_timestamp(F.col(column), expected_format)
    unparseable_count = df.filter(F.col(column).isNotNull() & parsed_col.isNull()).count()
    unparseable_ratio = unparseable_count / total_count if total_count > 0 else 0.0
    status = "PASS" if unparseable_ratio <= threshold else "FAIL"
    message = f"Column '{column}' has {unparseable_count} ({unparseable_ratio:.2%}) non-null values that do not match expected date format '{expected_format}'. Threshold: {threshold:.2%}"
    return {
        "status": status, "metric": unparseable_ratio, "message": message,
        "num_unparseable": unparseable_count, "total_rows": total_count
    }

def check_data_type(df: DataFrame, column: str, expected_spark_type_str: str, threshold: float = 0.0) -> dict:
    total_count = df.count()
    try:
        actual_type = df.schema[column].dataType
        actual_type_str = actual_type.simpleString()
    except (KeyError, IndexError):
         return {"status": "ERROR", "metric": 1.0, "message": f"Column '{column}' does not exist in the DataFrame schema.", "num_mismatch": 1, "total_rows": total_count}
    normalized_expected = expected_spark_type_str.strip().lower()
    is_match = actual_type_str == normalized_expected
    status = "PASS" if is_match else "FAIL"
    metric = 0.0 if is_match else 1.0
    message = f"Column '{column}' data type check. Expected: '{normalized_expected}', Actual: '{actual_type_str}'. Status: {status}"
    if metric > threshold:
        status = "FAIL"
    elif metric <= threshold:
        status = "PASS"
    return {
        "status": status, "metric": metric, "message": message,
        "num_mismatch": int(not is_match), "total_rows": total_count
    }
