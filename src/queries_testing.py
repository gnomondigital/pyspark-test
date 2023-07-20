from pyspark.sql import SparkSession
from pyspark.sql.functions import col,length
import pytest
import queries as q

# Define the test function
def test_add_address():
    # Create a SparkSession
    spark = SparkSession.builder.getOrCreate()

    # input data
    store_sales = [(1, 16.9), (2, 12.44), (3, 11.05)]
    sales_df = spark.createDataFrame(store_sales, ["Store", "Weekly_Sales"])
    sales_df.show()

    store_address = [(1, "address_1"), (2, "address_2"),(4,"address_4")]
    address_df = spark.createDataFrame(store_address, ["Store", "Address"])
    address_df.show()

    # expected outputs in the Adress column
    expected_address = ["address_1", "address_2"]

    # Apply a transformation to add store address
    transformed_df = q.add_address(sales_df,address_df)
    transformed_df.show()

    # Perform assertions to validate the transformation
    assert transformed_df.count() == 2
    assert "Address" in transformed_df.columns
    assert [row.Address for row in transformed_df.collect()] == expected_address

# Define the test function
def test_get_store_with_max_sales():
    # Create a SparkSession
    spark = SparkSession.builder.getOrCreate()

    # input data
    store_sales = [(1, 16.9), (2, 12.44), (3, 11.05), (3,7.4)]
    sales_df = spark.createDataFrame(store_sales, ["Store", "Weekly_Sales"])
    sales_df.show()


    # Apply a transformation to get store with max sales
    store = q.calculate_store_with_max_sales(sales_df)

    # Perform assertions to validate the transformation
    assert store == 3

# Define the test function
def test_extract_zip_code_from_address():
    # Create a SparkSession
    spark = SparkSession.builder.getOrCreate()

    # input data
    store_adress = [(1, "319 Kimberly Lane Suite 355, Zacharyfurt, RI 52945"),(2, "15638 Moore Summit, New Sarah, WI 48788")]
    address_df = spark.createDataFrame(store_adress, ["Store", "Address"])
    address_df.show()

    # expected outputs in the Adress column
    expected_zip_code = ["52945", "48788"]

    # Apply a transformation to create new column zip_code
    transformed_df = q.extract_zip_code(address_df)
    transformed_df.show()

    # Perform assertions to validate the transformation
    assert transformed_df.count() == 2
    assert "zip_code" in transformed_df.columns
    assert [row.zip_code for row in transformed_df.collect()] == expected_zip_code

def test_calculate_average_sales():
    # Create a SparkSession
    spark = SparkSession.builder.getOrCreate()

    # input data
    store_sales = [(1, 16.9), (2, 12.1), (3, 16.3)]
    sales_df = spark.createDataFrame(store_sales, ["Store", "Weekly_Sales"])
    sales_df.show()


    # Apply a transformation to calculate average sales
    average_sales = q.calculate_average_sales(sales_df)

    # Perform assertions to validate the transformation
    assert average_sales == 15.1

# Define the test function
def test_classify_sales():
    # Create a SparkSession
    spark = SparkSession.builder.getOrCreate()

    # input data
    store_sales = [(1, 16.9), (2, 12.1), (3, 16.3)]
    sales_df = spark.createDataFrame(store_sales, ["Store", "Weekly_Sales"])
    sales_df.show()


    # expected outputs in the Adress column
    expected_class = ["High sales", "Low sales", "High sales"]

    # Apply a transformation to classify sales
    transformed_df = q.classify_sales(sales_df,15.1)
    transformed_df.show()

    # Perform assertions to validate the transformation
    assert transformed_df.count() == 3
    assert "classification" in transformed_df.columns
    assert [row.classification for row in transformed_df.collect()] == expected_class

def test_calculate_month_with_min_sales():
    # Create a SparkSession
    spark = SparkSession.builder.getOrCreate()

    # input data
    store_sales = [(1,"05-01-2010",16.9), (2,"06-01-2010",12.1), (3,"05-02-2010",16.3)]
    sales_df = spark.createDataFrame(store_sales, ["Store", "Date", "Weekly_Sales"])
    sales_df.show()


    # Apply a transformation to get month with min sales
    month_with_min_sales = q.calculate_month_with_min_sales(store_sales)

    # Perform assertions to validate the transformation
    assert month_with_min_sales == "02"

# Run the test function
pytest.main([__file__])
