## Python 

This Python task involves fetching data from API, JSON data handling, working with parquet files
and performing simple calculations. Also, it requires basic git skills to upload the results. 
You will need to compare two product lists - actual data from url and expected data stored in parquet file.

### Actual data
Get products data from `https://dummyjson.com/products`
- **Total amount of products > 100** (use query parameters in the above path)
- Final price for each product can be calculated using 2 fields from the response - "price" and "discountPercentage"
-  Use different path if you want to save actual data.

### Expected data 
Stored in `./data/product_prices_calculated.parquet`
- Final price in expected data has been rounded to 2 decimal places

### Questions
1. What product is the most expensive according to actual data?
2. What product is missing in expected data?
3. For how many rows final price in expected data matches with calculated price from actual data?



**Pay attention, that your code should follow PEP8 standards.** 
- It should be easy to read, effective and explicit.
- It's recommended to create separate functions for each step of your solution (modular programming). 
Like:
```
get_actual_data()
upload_expected_data()
find_most_expensive_product()
...
```

