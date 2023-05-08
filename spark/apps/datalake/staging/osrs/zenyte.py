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
    
    app_name = "zenyte"
    # spark = SparkSession.builder.appName(app_name) \
          #.config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
          #.config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
          #.config("spark.hadoop.fs.s3a.endpoint", f"s3a://{aws_region}.amazonaws.com") \
          #.config("spark.hadoop.fs.s3a.connection.maximum", "100") \
          #.config("spark.hadoop.fs.s3a.block.size", "128m") \
          # .getOrCreate()
          
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    spark.conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.0")
    
    
    # Begin data processing
    item_ids = { 
                "zenyte_shard": 19529#, 
              #  "uncut_zenyte": 19496, 
              #  "zenyte": 19493,
              #  "amulet_of_torture": 19553,
              #  "necklace_of_anguish": 19547,
              #  "tormented_bracelet": 19544, 
              #  "ring_of_suffering": 19550
                }

    target_df = spark.createDataFrame([], ge_item_schema)
    target_location = f"spark-data/datalake/landing/lnd_grand_exchange_{timestamp}.json"
    # target_location = "s3a://wells-gaming-datalake/datalake/zenyte.parquet"

    # looping through desired item_id's from api
    for item_id in list(item_ids.values()):
        url = f'https://secure.runescape.com/m=itemdb_oldschool/api/catalogue/detail.json?item={item_id}'
        data = requests.get(url).json()
        df = spark.read.option("multiline", "true").json(sc.parallelize([data]))
        
        # flatten json format into df
        df = df.select(flatten(df.schema))
         
        # append data into our target df
        target_df = target_df.union(df)
        
    print(f'*********************{sc.getConf().get("spark.jars.packages")}******************')    
        
    target_df.show()
    print(f'---------- {target_df.count()} ----------')
    target_df\
        .coalesce(1)\
        .write\
        .mode('overwrite')\
        .parquet(target_location)
        
    spark.stop()
        
        
if __name__ == '__main__':
    main()
