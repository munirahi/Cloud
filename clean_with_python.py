import dask.dataframe as dd

# Read the CSV file
df = dd.read_csv('2019-Oct.csv')

# View a sample of the data before cleaning
print("Sample of the data before cleaning:")
print(df.head(5))  # Display the first 5 rows

# Print the number of records before cleaning
print(f"Total records before cleaning: {df.shape[0].compute()}")

# Drop rows where 'category_code' or 'price' are null or price <= 0
df_cleaned = df.drop_duplicates()
df_cleaned = df_cleaned[df_cleaned['category_code'].notnull() & (df_cleaned['price'] > 0)]

# Print the number of records after cleaning
print(f"Total records after cleaning: {df_cleaned.shape[0].compute()}")

# View a sample of the cleaned data
print("Sample of the cleaned data:")
print(df_cleaned.head(5))

# Save the cleaned data to a new CSV file
df_cleaned.to_csv('cleaned_data.csv', index=False, single_file=True)
