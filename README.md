Walmart is the biggest retail store in the United States. Just like others, they have been expanding their e-commerce part of the business. By the end of 2022, e-commerce represented a roaring $80 billion in sales, 
which is 13% of total sales of Walmart. One of the main factors that affects their sales is public holidays, like the Super Bowl, Labour Day, Thanksgiving, and Christmas.

In this project, I am tasked with creating a data pipeline for the analysis of supply and demand around the holidays, along with conducting a preliminary analysis of the data. I worked with two data sources: 
grocery sales and complementary data. 

    "index" - unique ID of the row
    "Store_ID" - the store number
    "Date" - the week of sales
    "Weekly_Sales" - sales for the given store

Also, I have the extra_data.parquet file that contains complementary data:
extra_data.parquet

    "IsHoliday" - Whether the week contains a public holiday - 1 if yes, 0 if no.
    "Temperature" - Temperature on the day of sale
    "Fuel_Price" - Cost of fuel in the region
    "CPI" – Prevailing consumer price index
    "Unemployment" - The prevailing unemployment rate
    "MarkDown1", "MarkDown2", "MarkDown3", "MarkDown4" - number of promotional markdowns
    "Dept" - Department Number in each store
    "Size" - size of the store
    "Type" - type of the store (depends on Size column)

I merged those files and performed some data manipulations. The transformed DataFrame was then stored as the clean_data variable containing the following columns:

    "Store_ID"
    "Month"
    "Dept"
    "IsHoliday"
    "Weekly_Sales"
    "CPI"
    ""Unemployment""

After merging and cleaning the data, I analyzed monthly sales of Walmart and stored the results of your analysis as the agg_data variable that should look like:


Finally, I loaded the clean_data and agg_data as csv files.
