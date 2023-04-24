import configparser
import os
from pyspark.sql import SparkSession


def main():
    # AWS config
    config_path = os.path.join(os.getcwd(), 'utils', 'aws.ini')
    config = configparser.ConfigParser()

    config.read(config_path)

    aws_access_key = config.get("s3", "access_key")
    aws_secret_key = config.get("s3", "secret_key")
    aws_region = config.get("s3", "region")    


    # Create a SparkSession
    spark = SparkSession.builder \
            .appName("test_s3_conn") \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", f"s3a.{aws_region}.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()

    # Create some sample data as a DataFrame
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["name", "age"])
    target_location = "s3a://wells-gaming-datalake/datalake/test_s3_conn.csv"

    df.write \
        .mode("overwrite") \
        .csv(target_location, header=True)
    spark.stop()
    
if __name__ == "__main__":
    main()