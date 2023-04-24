from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType


def main():
    # spark configs
    app_name = "dim_bis_jewelery"
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    
    # read file into table
    zenyte_file = '/tmp/output/zenyte.parquet'
    
    ge_item_schema = StructType([
    StructField('current_price', StringType(), True), #0
    StructField('current_trend', StringType(), True), #1
    StructField('day180_change', StringType(), True), #2
    StructField('day180_trend', StringType(), True),  #3
    StructField('day30_change', StringType(), True),  #4
    StructField('day30_trend', StringType(), True),   #5
    StructField('day90_change', StringType(), True),  #6
    StructField('day90_trend', StringType(), True),   #7
    StructField('description', StringType(), True),   #8
    StructField('icon', StringType(), True),          #9
    StructField('icon_large', StringType(), True),    #10
    StructField('id', LongType(), True),              #11
    StructField('members', StringType(), True),       #12
    StructField('name', StringType(), True),          #13
    StructField('today_price', StringType(), True),   #14
    StructField('today_trend', StringType(), True),   #15
    StructField('type', StringType(), True),          #16
    StructField('typeIcon', StringType(), True)       #17
])
    
    df = spark.read\
        .schema(ge_item_schema)\
        .parquet(zenyte_file)
        #.createOrReplaceTempView("src_zenyte")        #.schema(ge_item_schema)\
        
    print(f' ------------------------- {df.count()} -------------------------')
    spark.stop()
    
if __name__ == "__main__":
    main()