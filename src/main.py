from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import sum, col, desc, regexp_extract, udf, mean, lit, to_date, substring, asc
from pyspark.sql.types import StringType


spark = SparkSession.builder.master("local[1]").appName("workshop-Walmart").getOrCreate()

def read_csv(path: str)-> DataFrame:
    return spark.read.option("header",True).option("inferSchema",True) \
        .option("delimiter",",") \
        .csv(path)

walmart_store_sales = read_csv("./data/Walmart_Store_sales.csv")
store_address = read_csv("./data/store_address.csv")

output_df = walmart_store_sales.join(store_address,"Store", "inner")


store_with_max_sales = output_df.groupBy("Store") \
                                .agg(sum("Weekly_Sales").alias("sum_sales")) \
                                .orderBy(col("sum_sales").desc()) \
                                .first()["Store"]

print(store_with_max_sales)

store_address.withColumn("zip_code",regexp_extract(store_address.Address, r'(\d+)$', 1)) \
    .show()


#if the total sales is higher than the mean of  all sales then classification= "High sales"
#  otherwise classification = "Low Sales"

@udf(returnType=StringType()) 
def classificationUDF(store_sales,mean):
    if store_sales > mean:
        return "High sales"
    else:
        return "Low sales"

average_sales = walmart_store_sales.select(mean(col("Weekly_Sales")).alias("average_sales")) \
    .first()["average_sales"]

walmart_store_sales.select(col("Store"), classificationUDF(col("Weekly_Sales"), lit(average_sales)).alias("classification")) \
                   .show()
                      
#get the month that has the minimum sales

month_with_min_sales = walmart_store_sales.withColumn("month", substring("Date", 4,8)) \
                                .groupBy("month") \
                                .agg(sum("Weekly_Sales").alias("sum_sales")) \
                                .orderBy(col("sum_sales").asc()) \
                                .first()["month"]

print(month_with_min_sales)
