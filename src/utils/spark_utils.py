# Spark工具：SparkSession工厂方法(local[*], driver 2GB, timeParserPolicy=LEGACY)，提供创建/关闭便捷函数
from pyspark.sql import SparkSession

def get_spark_session(app_name="DataQualityApp"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark Session '{app_name}' initialized inside container.")
    print(f"Spark version: {spark.version}")
    return spark

def stop_spark_session(spark: SparkSession):
    if spark:
        print("Stopping Spark Session.")
        spark.stop()
    else:
        print("No active Spark Session to stop.")
