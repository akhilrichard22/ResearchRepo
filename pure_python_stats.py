# Databricks notebook source
# Alternative with file URI scheme
file_path = "file:/Workspace/Users/akvincen@syr.edu/facebook_ads.csv"

ads_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Display basic info
display(ads_df)
print(f"Total records: {ads_df.count()}")

# 1. Overall descriptive statistics
from pyspark.sql.functions import col, mean, min, max, stddev, countDistinct, count, first

# Numeric columns analysis
numeric_cols = ['estimated_audience_size', 'estimated_impressions', 'estimated_spend']
for column in numeric_cols:
    print(f"\nStatistics for {column}:")
    display(ads_df.select(
        mean(col(column)).alias("mean"),
        min(col(column)).alias("min"),
        max(col(column)).alias("max"),
        stddev(col(column)).alias("stddev")
    ))
    
# Categorical columns analysis
categorical_cols = ['currency', 'bylines']
for column in categorical_cols:
    print(f"\nStatistics for {column}:")
    display(ads_df.groupBy(column).count().orderBy("count", ascending=False).limit(5))

# 2. Group by page_id
print("\nGrouped by page_id:")
page_stats = ads_df.groupBy("page_id").agg(
    mean("estimated_spend").alias("avg_spend"),
    min("estimated_spend").alias("min_spend"),
    max("estimated_spend").alias("max_spend"),
    count("ad_id").alias("ad_count")
)
display(page_stats)

# 3. Group by (page_id, ad_id)
print("\nGrouped by (page_id, ad_id):")
ad_stats = ads_df.groupBy("page_id", "ad_id").agg(
    mean("estimated_spend").alias("avg_spend"),
    min("estimated_spend").alias("min_spend"),
    max("estimated_spend").alias("max_spend"),
    first("ad_creation_time").alias("creation_time")
)
display(ad_stats)