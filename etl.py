import pandas as pd
import os
from sqlalchemy import create_engine

def extract(grocery_sales_path, extra_data_path):
    extra_data_df = pd.read_parquet(extra_data_path)

    # Create the SQLAlchemy engine for SQLite
    engine = create_engine(f'sqlite:///{grocery_sales_path}')
    
    # Read data from the grocery_sales table
    grocery_sales_df = pd.read_sql("SELECT * FROM grocery_sales", engine)
    
    # Assuming that Store_ID is the common column to merge on
    merged_df = pd.merge(grocery_sales_df, extra_data_df, on='Store_ID')
    
    return merged_df

# Provide the SQLite database path
grocery_sales_path = 'grocery_sales.db'
extra_data_path = 'extra_data.parquet'

# Extract the data
merged_df = extract(grocery_sales_path, extra_data_path)

def transform(merged_df):
    # Check if columns exist before filling NaN values
    fillna_dict = {}
    if 'Unemployment' in merged_df.columns:
        fillna_dict['Unemployment'] = merged_df['Unemployment'].mean()
    if 'Type' in merged_df.columns:
        fillna_dict['Type'] = merged_df['Type'].mode()[0]
    if 'Size' in merged_df.columns:
        fillna_dict['Size'] = merged_df['Size'].mode()[0]
    if 'CPI' in merged_df.columns:
        fillna_dict['CPI'] = merged_df['CPI'].mean()
    
    clean_data = merged_df.fillna(fillna_dict)
    
    # Convert 'Date' to datetime format and extract month
    clean_data['Date'] = pd.to_datetime(clean_data['Date'], format="%Y-%m-%d")
    clean_data['Month'] = clean_data['Date'].dt.month
    
    # Extract holiday months
    holiday_months = clean_data.loc[clean_data['IsHoliday'] == 1, 'Date'].dt.month.unique()
    
    # Find the months preceding and following the holiday months
    preceding_months = [(month - 1) % 12 or 12 for month in holiday_months]
    following_months = [(month + 1) % 12 or 12 for month in holiday_months]
    
    # Combine all relevant months into a single list
    relevant_months = list(set(holiday_months.tolist() + preceding_months + following_months))
    
    # Filter rows based on 'Weekly_Sales' and 'Month'
    filtered_df = clean_data.loc[
        (clean_data['Weekly_Sales'] > 10000) & 
        (clean_data['Month'].isin(relevant_months))
    ]
    
    # Keep only the required columns
    clean_data = filtered_df[['Store_ID', 'Month', 'Dept', 'IsHoliday', 'Weekly_Sales', 'CPI', 'Unemployment']]
    
    # Drop the index column if it exists
    if 'index' in clean_data.columns:
        clean_data = clean_data.drop(columns=['index'])
    
    return clean_data

# Assuming merged_df is your input DataFrame
clean_data = transform(merged_df)

def avg_monthly_sales(clean_data):
    analysis_data = clean_data[['Month', 'Weekly_Sales']]
    
    Avg_Sales = analysis_data.groupby('Month').mean()
    new_data = Avg_Sales.round(2).rename(columns={'Weekly_Sales': 'Avg_Sales'})
    return new_data 
    
agg_data = avg_monthly_sales(clean_data)
print(agg_data)

def load(clean_data, agg_data, clean_data_path, agg_data_path):
    clean_data.to_csv(clean_data_path, index=False)
    agg_data.to_csv(agg_data_path, index=False)

# Assuming you want to save the data to 'clean_data.csv' and 'agg_data.csv'
load(clean_data, agg_data, "clean_data.csv", "agg_data.csv")

def validation(clean_data_path):
    if os.path.exists(clean_data_path):
        print('File Exists')
    else:
        print('File DOES NOT Exist')
    return clean_data_path

# Validate that the clean data CSV file was saved
validation("clean_data.csv")
