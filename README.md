# ResearchRepo

# README for Facebook Ads Analysis Project

## Project Overview
This repository contains three implementations for analyzing Facebook ad data from the 2024 US Presidential election. Each implementation uses a different Python library to perform descriptive statistics on the dataset.

## Dataset Information
- **Source**: Facebook ads run during the 2024 USA Presidential election
- **Size**: 246,745 records
- **Key Columns**:
  - `page_id`: Identifier for the organization running the ad
  - `ad_id`: Unique identifier for each ad
  - `currency`: Currency used for payment (primarily USD)
  - `bylines`: Organization responsible for the ad
  - `estimated_spend`: Amount spent on the ad
  - `estimated_impressions`: Number of times the ad was shown
  - `estimated_audience_size`: Target audience size

## How to Run the Code

### Prerequisites
- Databricks workspace or local Python environment
- Dataset file: `facebook_ads.csv`

### Running in Databricks
1. Upload the dataset to your Databricks workspace:
   - Path: `/Workspace/Users/your-username/facebook_ads.csv`
2. Import the notebooks:
   - [PySpark Notebook](pyspark_analysis.ipynb)
   - [Pandas Notebook](pandas_analysis.ipynb)
   - [Polars Notebook](polars_analysis.ipynb)
3. For each notebook:
   - Update the `file_path` variable with your username
   - Run all cells sequentially

### Running Locally
1. Install requirements:
   ```bash
   pip install pyspark pandas polars notebook
   ```
2. Run Jupyter Notebook:
   ```bash
   jupyter notebook
   ```
3. Open and run each notebook:
   - PySpark: `pyspark_analysis.ipynb`
   - Pandas: `pandas_analysis.ipynb`
   - Polars: `polars_analysis.ipynb`

## Key Findings

### 1. Overall Statistics
- **Total Ads**: 246,745
- **Currency Distribution**:
  - USD: 246,599 (99.94%)
  - Other currencies (INR, GBP, EUR, EGP): 146 (0.06%)

### 2. Top Advertisers
1. HARRIS FOR PRESIDENT: 49,788 ads
2. HARRIS VICTORY FUND: 32,612 ads
3. BIDEN VICTORY FUND: 15,539 ads
4. DONALD J. TRUMP FOR PRESIDENT 2024, INC.: 15,112 ads
5. Trump National Committee JFC: 7,279 ads

### 3. Spending Analysis
- Significant variation in ad spending across organizations
- Most ads have relatively small budgets, with a few high-spending campaigns

### 4. Page Analysis
- 4,475 unique organizations (page_id) ran ads
- Most organizations ran multiple ads, with top organizations running thousands of ads

### 5. Platform Usage
- Facebook and Instagram were the dominant platforms for ad distribution
- Some ads used additional platforms like Audience Network and Messenger

## Interesting Insights

1. **Political Dominance**: The top 5 advertisers account for approximately 50% of all ads, showing concentrated political spending.

2. **Currency Uniformity**: The overwhelming use of USD (99.94%) suggests these ads primarily targeted US audiences.

3. **Harris Campaign Dominance**: Organizations related to Kamala Harris accounted for 3 of the top 5 advertisers and the top 2 positions.

4. **Bipartisan Presence**: Both Democratic (Harris, Biden) and Republican (Trump) campaigns were among the top advertisers.

5. **Long-tail Distribution**: While 4,475 organizations ran ads, the top 5 organizations accounted for a disproportionate share of total ads.

6. **Ad Volume**: The high number of ads (246,745) indicates a highly competitive digital advertising landscape during the election.

## Recommendations for Further Analysis
1. Investigate temporal patterns - how ad volume changed during key election milestones
2. Analyze regional targeting strategies using the `delivery_by_region` data
3. Study demographic targeting using the `demographic_distribution` data
4. Compare effectiveness metrics (impressions vs spend) across different advertisers
5. Examine messaging trends using the topic classification columns

## Note
The dataset file `facebook_ads.csv` is not included in this repository per the project requirements. You must obtain the dataset separately and place it in the appropriate location before running the analysis.
