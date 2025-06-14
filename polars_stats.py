# Databricks notebook source
# Databricks notebook source
# DBTITLE 1,Facebook Ads Analysis with Polars
# MAGIC %md
# MAGIC # Facebook Ads Analysis with Polars
# MAGIC 
# MAGIC This notebook performs descriptive statistics on the Facebook Ads dataset using Polars.

# COMMAND ----------

# DBTITLE 1,Step 1: Install and Import Polars
# Install polars
%pip install polars

# Import required libraries
import polars as pl

# COMMAND ----------

# DBTITLE 1,Step 2: Load Data
# File path to the dataset
file_path = "/Workspace/Users/akvincen@syr.edu/facebook_ads.csv"

# Read CSV file
try:
    polars_df = pl.read_csv(file_path)
    print(f"Total records: {polars_df.height}")
    display(polars_df.head())
except Exception as e:
    print(f"Error loading data: {e}")
    # Debug: Show files in directory
    print("Files in directory:")
    dbutils.fs.ls("/Workspace/Users/akvincen@syr.edu/")

# COMMAND ----------

# DBTITLE 1,Step 3: Overall Descriptive Statistics
# Define numeric columns to analyze
numeric_cols = ['estimated_audience_size', 'estimated_impressions', 'estimated_spend']

# Calculate statistics
try:
    stats = polars_df.select([
        pl.col('estimated_audience_size').mean().alias('audience_mean'),
        pl.col('estimated_audience_size').min().alias('audience_min'),
        pl.col('estimated_audience_size').max().alias('audience_max'),
        pl.col('estimated_impressions').mean().alias('impressions_mean'),
        pl.col('estimated_impressions').min().alias('impressions_min'),
        pl.col('estimated_impressions').max().alias('impressions_max'),
        pl.col('estimated_spend').mean().alias('spend_mean'),
        pl.col('estimated_spend').min().alias('spend_min'),
        pl.col('estimated_spend').max().alias('spend_max'),
    ])

    print("Overall numeric statistics:")
    display(stats)
except Exception as e:
    print(f"Error calculating numeric stats: {e}")
    print("Available columns:", polars_df.columns)

# Categorical columns analysis
categorical_cols = ['currency', 'bylines']
for col_name in categorical_cols:
    try:
        print(f"\nValue counts for '{col_name}':")
        # Robust value counts with column name check
        if col_name in polars_df.columns:
            value_counts = polars_df[col_name].value_counts()
            display(value_counts.sort(by='count', descending=True).head(5))
        else:
            print(f"Column '{col_name}' not found in dataframe")
    except Exception as e:
        print(f"Error processing '{col_name}': {e}")

# COMMAND ----------

# DBTITLE 1,Step 4: Group by page_id
try:
    # Group by page_id and aggregate
    page_group = polars_df.group_by('page_id').agg(
        pl.col('estimated_spend').mean().alias('avg_spend'),
        pl.col('estimated_spend').min().alias('min_spend'),
        pl.col('estimated_spend').max().alias('max_spend'),
        pl.count().alias('ad_count')
    )

    print("Grouped by page_id:")
    display(page_group)
except Exception as e:
    print(f"Error grouping by page_id: {e}")

# COMMAND ----------

# DBTITLE 1,Step 5: Group by (page_id, ad_id)
try:
    # Group by both page_id and ad_id
    ad_group = polars_df.group_by(['page_id', 'ad_id']).agg(
        pl.col('ad_creation_time').first().alias('creation_time'),
        pl.col('estimated_spend').mean().alias('avg_spend'),
        pl.col('estimated_spend').min().alias('min_spend'),
        pl.col('estimated_spend').max().alias('max_spend')
    )

    print("Grouped by (page_id, ad_id):")
    display(ad_group.head(10))
except Exception as e:
    print(f"Error grouping by (page_id, ad_id): {e}")

# COMMAND ----------