from pyspark.sql import SparkSession

def main():
    # spark configs
    app_name = "dim_bis_jewelery"
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    
    # read file into table
    zenyte_file = '/tmp/output/zenyte.parquet'
    
    spark.read\
        .parquet('/tmp/output/zenyte.parquet/')\
        .show()
        #.createOrReplaceTempView("src_zenyte")
    
if __name__ == "__main__":
    main()