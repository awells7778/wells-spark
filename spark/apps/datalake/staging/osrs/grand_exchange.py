import requests
import configparser
import os
import boto3
from utils.grand_exchange import flatten, ge_item_schema
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col


def main():
    
    # AWS config
    
    config_path = os.path.join(os.getcwd(), 'utils', 'aws.ini')
    config = configparser.ConfigParser()
    
    config.read(config_path)

    # aws_access_key = config.get("s3", "access_key")
    # aws_secret_key = config.get("s3", "secret_key")
    # aws_region = config.get("s3", "region")    
    # aws_session = boto3.Session(aws_access_key_id=aws_access_key,
    #                             aws_secret_access_key=aws_secret_key,
    #                             region_name=aws_region)
    
    # Spark session setup
    
    app_name = "grand_exchange"
    # spark = SparkSession.builder.appName(app_name) \
          #.config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
          #.config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
          #.config("spark.hadoop.fs.s3a.endpoint", f"s3a://{aws_region}.amazonaws.com") \
          #.config("spark.hadoop.fs.s3a.connection.maximum", "100") \
          #.config("spark.hadoop.fs.s3a.block.size", "128m") \
          # .getOrCreate()