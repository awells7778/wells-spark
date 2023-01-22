from pyspark.sql import SparkSession


def init_spark():
  sql = SparkSession.builder\
        .appName("sample-app")\
        .getOrCreate()
  sc = sql.sparkContext
  return sql, sc

      #  .config("spark.jars", "/opt/spark-apps/postgresql-42.2.22.jar")\