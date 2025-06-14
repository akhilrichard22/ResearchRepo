# Databricks notebook source
# Databricks notebook source
# DBTITLE 1,Facebook Ads Analysis with Pandas
# MAGIC %md
# MAGIC # Facebook Ads Analysis with Pandas
# MAGIC 
# MAGIC This notebook performs descriptive statistics on the Facebook Ads dataset using Pandas.

# COMMAND ----------

# DBTITLE 1,Step 1: Install and Import Pandas
# Install pandas if not already available
%pip install pandas

# Import required libraries
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Step 2: Load Data
# File path to the dataset
file_path = "/Workspace/Users/akvincen@syr.edu/facebook_ads.csv"

# Read CSV file
pdf = pd.read_csv(file_path)

# Display basic information
print(f"Total records: {len(pdf)}")
display(pdf.head())

# COMMAND ----------

# DBTITLE 1,Step 3: Overall Descriptive Statistics
# Define numeric columns to analyze
numeric_cols = ['estimated_audience_size', 'estimated_impressions', 'estimated_spend']

# Calculate and display statistics
print("Descriptive statistics for numeric columns:")
display(pdf[numeric_cols].describe())

# Categorical columns analysis
categorical_cols = ['currency', 'bylines']
for col_name in categorical_cols:
    print(f"\nValue counts for '{col_name}':")
    display(pdf[col_name].value_counts().head(5))

# COMMAND ----------

# DBTITLE 1,Step 4: Group by page_id
# Group by page_id and aggregate
page_group = pdf.groupby('page_id')['estimated_spend'].agg(
    ['mean', 'min', 'max', 'count']
).rename(columns={
    'mean': 'avg_spend',
    'min': 'min_spend',
    'max': 'max_spend',
    'count': 'ad_count'
})

print("Grouped by page_id:")
display(page_group)

# COMMAND ----------

# DBTITLE 1,Step 5: Group by (page_id, ad_id)
# Group by both page_id and ad_id
ad_group = pdf.groupby(['page_id', 'ad_id']).agg({
    'ad_creation_time': 'first',
    'estimated_spend': ['mean', 'min', 'max']
})

# Flatten multi-index columns
ad_group.columns = ['_'.join(col).strip() for col in ad_group.columns.values]

# Rename columns
ad_group = ad_group.rename(columns={
    'ad_creation_time_first': 'creation_time',
    'estimated_spend_mean': 'avg_spend',
    'estimated_spend_min': 'min_spend',
    'estimated_spend_max': 'max_spend'
})

print("Grouped by (page_id, ad_id):")
display(ad_group.head(10))

# COMMAND ----------