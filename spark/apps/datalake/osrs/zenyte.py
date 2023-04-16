import requests
from utils.grand_exchange import flatten, ge_item_schema
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col


def main():
    # Spark session setup
    app_name = 'zenyte'
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    
    # Begin data processing
    item_ids = { 
                "zenyte_shard": 19529, 
                "uncut_zenyte": 19496, 
                "zenyte": 19493,
                "amulet_of_torture": 19553,
                "necklace_of_anguish": 19547,
                "tormented_bracelet": 19544, 
                "ring_of_suffering": 19550
                }

    target_df = spark.createDataFrame([], ge_item_schema)
    #target_file = "/tmp/output/zenyte.parquet"

    # looping through desired item_id's from api
    for item_id in list(item_ids.values()):
        url = f'https://secure.runescape.com/m=itemdb_oldschool/api/catalogue/detail.json?item={item_id}'
        data = requests.get(url).json()
        df = spark.read.option("multiline", "true").json(sc.parallelize([data]))
        
        # flatten json format into df
        df = df.select(flatten(df.schema))
         
        # append data into our target df
        target_df = target_df.union(df)
        
    target_df.show()
    target_df\
        .coalesce(1)\
        .write\
        .csv('/tmp/output/zenyte.csv')
    
    spark.read\
    .csv('/tmp/output/zenyte.csv')\
    .show()
        
        
if __name__ == '__main__':
    main()
