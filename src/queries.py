from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import sum, col, desc, regexp_extract, udf, mean, lit, to_date, substring, asc
from pyspark.sql.types import StringType

spark = SparkSession.builder.master("local[1]").appName("workshop-Walmart").getOrCreate()

def read_csv(path: str)-> DataFrame:
    return spark.read.option("header",True).option("inferSchema",True) \
        .option("delimiter",",") \
        .csv(path)


def add_address(sales_df: DataFrame, address_df: DataFrame)-> DataFrame:
    return sales_df.join(address_df,"Store", "inner")


def calculate_store_with_max_sales(sales_df: DataFrame)-> float:
    return sales_df.groupBy("Store") \
                    .agg(sum("Weekly_Sales").alias("sum_sales")) \
                    .orderBy(col("sum_sales").desc()) \
                    .first()["Store"]


def extract_zip_code(sales_df: DataFrame)-> DataFrame:
    return sales_df.withColumn("zip_code",regexp_extract(sales_df.Address, r'(\d+)$', 1))


@udf(returnType=StringType()) 
def classificationUDF(store_sales,mean_sales):
    if store_sales > mean_sales:
        return "High sales"
    else:
        return "Low sales"


def calculate_average_sales(sales_df: DataFrame)-> float:
    return sales_df.select(mean(col("Weekly_Sales")).alias("average_sales")) \
                                       .first()["average_sales"]

def classify_sales(sales_df: DataFrame, average_sales: float)-> DataFrame:
    return sales_df.select(col("Store"), classificationUDF(col("Weekly_Sales"), lit(average_sales)).alias("classification"))


def calculate_month_with_min_sales(sales_df: DataFrame)-> int:
    return sales_df.withColumn("month", substring("Date", 4,2)) \
                   .groupBy("month") \
                   .agg(sum("Weekly_Sales").alias("sum_sales")) \
                   .orderBy(col("sum_sales").asc()) \
                   .first()["month"]
