import queries as q

def main():
    #load csv file into dataframe
    walmart_store_sales = q.read_csv("./data/Walmart_Store_sales.csv")
    store_address = q.read_csv("./data/store_address.csv")

    #join store_address to get store address
    output_df = q.add_address(walmart_store_sales,store_address)
    output_df.show()

    #find the store with max sales
    store_with_max_sales = q.calculate_store_with_max_sales(output_df)
    print(f"The store with max sales is {store_with_max_sales}")

    #extract zip_code from address
    output_df_with_zip_code = q.extract_zip_code(output_df)
    output_df.show()

    #if the total sales is higher than the mean of  all sales then classification= "High sales"
    #  otherwise classification = "Low Sales"
    average_sales = q.calculate_average_sales(output_df_with_zip_code)
    output_df_classified = q.classify_sales(output_df_with_zip_code, average_sales)
    output_df_classified.show()

    #get the month that has the minimum sales
    month_with_min_sales = q.calculate_month_with_min_sales(output_df_with_zip_code)
    print(f"the month that has the minimum sales is {month_with_min_sales}")


if __name__ == "__main__":
    main()
