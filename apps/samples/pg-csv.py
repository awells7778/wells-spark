
"""
pg-csv.py: This ETL job takes a small flat file, and loads it into the postgres
database. This file is a modified version of the main.py file found within 
mvillarealb's https://github.com/mvillarrealb/docker-spark-cluster/.  It serves
as a baseline example of a spark application.  Use pg.sh to run this job.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format

def init_spark():
  sql = SparkSession.builder\
        .appName("trip-app")\
        .config("spark.jars", "/opt/spark-apps/postgresql-42.2.22.jar")\
        .getOrCreate()
  sc = sql.sparkContext
  return sql, sc

def main():
  url = "jdbc:postgresql://demo-database:5432/postgres"
  properties = {
    "user": "postgres",
    "password": "casa1234",
    "driver": "org.postgresql.Driver"
  }
  file = "/opt/spark-data/us-500.csv"
  sql, sc = init_spark()

  df = sql.read.load(file,format = "csv", inferSchema="true", sep=",", header="true")
  
  # Filter invalid coordinates
  df.write \
    .jdbc(url=url, table="wdw_landing.us_500", mode='append', properties=properties) \
    .save()
  
if __name__ == '__main__':
  main()