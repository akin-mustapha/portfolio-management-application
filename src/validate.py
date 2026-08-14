from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("local-test") \
    .master("local[*]") \
    .getOrCreate()

    
    
df  = spark.read.parquet("run-1784300779806-part-block-0-0-r-00009-snappy.parquet")




df.printSchema()
df.show()