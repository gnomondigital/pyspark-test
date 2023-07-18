from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import sum, col, desc, regexp_extract, udf, mean
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

store_address.withColumn("ZIP_code",regexp_extract(store_address.Address, r'(\d+)$', 1)).show()


#if the total sales is higher than the mean of  all sales then classification= "High sales"
#  otherwise classification = "Low Sales"

@udf(returnType=StringType()) 
def classificationUDF(store_sales,mean):
    if store_sales > mean:
        return "High sales"
    else:
        return "Low sales"
    
walmart_store_sales.select(col("Store"), \
                           classificationUDF(col("Weekly_Sales"),mean(col("Weekly_Sales"))) \
                            .alias("classification")).show(truncate=False)
                      
#get the month that has the minimum sales

#add comments
