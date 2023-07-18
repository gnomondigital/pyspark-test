from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("workshop-Walmart").getOrCreate()

store_address = spark.read.option("header",True).option("inferSchema",True) \
    .option("delimiter",",") \
        .csv("./data/store_address.csv")

store_address.show()

walmart_store_sales = spark.read.option("header",True).option("inferSchema",True) \
    .option("delimiter",",") \
        .csv("./data/Walmart_Store_sales.csv")

walmart_store_sales.show()