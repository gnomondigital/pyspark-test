# README #

This repository contains a mini pyspark project for interview purpose.
 

# Summary #

The project is mainly about reading retail data ( walmart sales) and answer some questions to show 
the candidate's ability to code in pyspark
# Data #
The data in question has 2 files: 
1. Walmart_Store_sales.csv: contains weekly sales keyed by store and week, and a couple of other features
   that describes the data (Temperature, Holiday Flag, Fuel price, etc...)
3. store_address.csv: Has the address of each store.

# What the candidate should do #
1. Load the data into dataframes and join them into a single one.
2. Get the store that has the maximum sales
3. Apply a regular expression to extract the ZIP code from the address column
4. Get the month with the minimum sales
5. Apply a UDF to add a column named "classification" that has:
   "high_sales" if the total sales of a store is greater than the mean of the total sales
   "Low_sales" if the total sales of a store is lower than the mean of the total sale
6. Write unit tests for each instruction.
