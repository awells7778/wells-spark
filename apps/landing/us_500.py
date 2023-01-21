"""
pg-csv.py: This ETL job takes a small flat file, and loads it into the postgres
database. This file is a modified version of the main.py file found within 
mvillarealb's https://github.com/mvillarrealb/docker-spark-cluster/.  It serves
as a baseline example of a spark application.  Use pg.sh to run this job.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, current_timestamp, lit

def init_spark():
  sql = SparkSession.builder\
        .appName("sample-app")\
        .getOrCreate()
  sc = sql.sparkContext
  return sql, sc

      #  .config("spark.jars", "/opt/spark-apps/postgresql-42.2.22.jar")\

def main():
  url = "jdbc:postgresql://demo-database:5432/postgres"
  properties = {
    "user": "postgres",
    "password": "casa1234",
    "driver": "org.postgresql.Driver"
  }
  file = "/opt/spark-data/us_500.csv"
  sql, sc = init_spark()
  df = sql.read.load(file, format = "csv", inferSchema="true", sep=",", header="true")\
    .withColumn("load_timestamp", lit(current_timestamp()))
  
  # Save csv data to pg db
  df.write \
    .option("truncate", "true") \
    .jdbc(url=url, table="landing.us_500", mode="overwrite", properties=properties)
  
if __name__ == "__main__":
  main()