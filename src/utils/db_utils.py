# src/utils/db_utils.py
import psycopg2
import json
from datetime import datetime
import numpy as np
import traceback

def log_results_to_postgres(results, db_config, pipeline_run_id=None, data_source=None):
    required_keys = ["connection_string", "table_name"]
    if not all(key in db_config for key in required_keys):
        print("ERROR: Missing required database configuration keys (connection_string, table_name).")
        return
    conn_str = db_config.get("connection_string")
    table_name = db_config.get("table_name", "dq_results_log")
    if not conn_str:
        print("ERROR: Database connection string is empty or null.")
        return
    print(f"Attempting to log results to PostgreSQL table '{table_name}'...")
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(conn_str)
        conn.autocommit = False
        cur = conn.cursor()
        insert_sql = f"""
        INSERT INTO {table_name} (
            check_name, dq_check_timestamp, status, metric, message,
            parameters, details, data_source, pipeline_run_id, check_run_timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        log_count = 0
        overall_run_ts = datetime.now()
        for check_name, result in results.items():
            dq_check_ts_str = result.get("run_timestamp")
            dq_check_ts_obj = None
            if dq_check_ts_str:
                try:
                     dq_check_ts_obj = datetime.fromisoformat(dq_check_ts_str)
                except (TypeError, ValueError):
                     print(f"Warning: Could not parse timestamp '{dq_check_ts_str}' for check '{check_name}'. Setting to NULL.")
                     dq_check_ts_obj = None
            metric_val = result.get("metric")
            if isinstance(metric_val, float) and not np.isfinite(metric_val):
                 metric_serializable = None
            else:
                 metric_serializable = metric_val
            try:
                 params_dict = result.get("parameters", {})
                 params_json = json.dumps(params_dict)
            except TypeError as e:
                 print(f"Warning: Could not serialize parameters for check '{check_name}'. Storing empty JSON. Error: {e}")
                 params_json = '{}'
            try:
                 details_dict = {k: v for k, v in result.items() if k not in ['status', 'metric', 'message', 'parameters', 'run_timestamp']}
                 details_serializable = {}
                 for k, v in details_dict.items():
                     if isinstance(v, float) and not np.isfinite(v):
                         details_serializable[k] = None
                     else:
                         details_serializable[k] = v
                 details_json = json.dumps(details_serializable)
            except TypeError as e:
                 print(f"Warning: Could not serialize details for check '{check_name}'. Storing empty JSON. Error: {e}")
                 details_json = '{}'
            cur.execute(insert_sql, (
                check_name, dq_check_ts_obj,
                result.get("status", "ERROR"), metric_serializable,
                result.get("message", "N/A"), params_json, details_json,
                data_source, pipeline_run_id, overall_run_ts
            ))
            log_count += 1
        conn.commit()
        print(f"Successfully committed {log_count} DQ check results to PostgreSQL.")
    except psycopg2.Error as db_err:
        print(f"ERROR: Database error during logging: {db_err}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"ERROR: Unexpected error during database logging: {e}")
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("Database connection closed.")
