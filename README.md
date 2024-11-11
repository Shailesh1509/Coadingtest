# Coadingtest

QUERY_1: Return all months in the current year with exactly 30 days
SELECT iMonth, sMonth
FROM tblDimDate
WHERE iYear = YEAR(CURDATE())
GROUP BY iMonth, sMonth
HAVING COUNT(DISTINCT dateDay) = 30;

QUERY_2: Determine Missing Dates in tblDimDate
SELECT
    (DATEDIFF(MAX(dateDay), MIN(dateDay)) + 1) - COUNT(*) AS missing_dates
FROM tblDimDate;

QUERY_3: Identify Orders in November 2023 with No Records in tblAdvertiserLineItem
SELECT o.id, a.idAdvertiser, sStatus, o.dateStart, o.dateEnd
FROM tblOrder o
LEFT JOIN tblAdvertiserLineItem a ON o.id = a.idOrder
WHERE o.dateStart >= '2023-11-01'
  AND o.dateEnd <= '2023-11-30'
  AND a.idOrder IS NULL;

QUERY_4: Count Campaigns Grouped by Duration
This query calculates campaign duration as the difference between dateEnd and dateStart in days and counts campaigns by duration.
sql
Copy code
SELECT 
    DATEDIFF(dateEnd, dateStart) AS campaign_duration,
    COUNT(*) AS campaign_count
FROM tblOrder
GROUP BY campaign_duration;


Database Design Question:
Advantages and Disadvantages of Normalized Tables:
•	Advantages:
o	Data Consistency: Reduces redundancy, preventing anomalies.
o	Efficient Storage: Saves storage space by avoiding data duplication.
o	Easy Maintenance: Streamlined updates since data appears only once.
•	Disadvantages:
o	Complex Queries: Requires joins, which can slow down queries.
o	Performance Overhead: Join operations consume more resources.
o	Scalability Issues: Complex queries can slow down as data grows.
Advantages and Disadvantages of Non-Normalized Tables:
•	Advantages:
o	Faster Queries: Denormalized tables reduce joins, speeding up queries.
o	Simplified Data Retrieval: Data is often directly accessible.
o	Better for Analytical Workloads: Suited for read-heavy or reporting applications.
•	Disadvantages:
o	Data Redundancy: Increases storage requirements and can lead to inconsistency.
o	Data Maintenance Complexity: Updates become more challenging due to redundancy.
o	Storage Cost: Higher storage costs for duplicated data.










Data Validation and Analysis
import pandas as pd
import numpy as np
from collections import defaultdict

# Define the expected schema
schema = {
    "columns": ["id", "name", "age", "salary"],
    "delimiters": ",",  # Assuming CSV file with commas as delimiters
    "data_types": {"id": int, "name": str, "age": int, "salary": float},
}

def validate_schema(file_path, schema):
    exceptions = defaultdict(list)

    # Load data
    try:
        data = pd.read_csv(file_path, delimiter=schema["delimiters"])
    except Exception as e:
        print(f"Error loading file: {e}")
        return

    # Check columns
    missing_cols = set(schema["columns"]) - set(data.columns)
    extra_cols = set(data.columns) - set(schema["columns"])
    
    if missing_cols or extra_cols:
        exceptions["column_mismatch"].append({
            "missing_columns": list(missing_cols),
            "extra_columns": list(extra_cols)
        })
        print(f"Column issues found. Missing: {missing_cols}, Extra: {extra_cols}")
    
    # Validate data types
    for col, expected_type in schema["data_types"].items():
        if col in data.columns:
            incorrect_types = data[~data[col].apply(lambda x: isinstance(x, expected_type) or pd.isna(x))]
            if not incorrect_types.empty:
                example_value = incorrect_types.iloc[0][col]
                exceptions["type_mismatch"].append({
                    "column": col,
                    "expected_type": expected_type.__name__,
                    "invalid_count": incorrect_types.shape[0],
                    "example_invalid_value": example_value,
                })
    
    # Display results
    if exceptions:
        for key, error_list in exceptions.items():
            for error in error_list:
                print(f"Issue: {key}, Details: {error}")
    else:
        print("All records conform to schema.")
    
# Run validation
validate_schema("sample_data.csv", schema)






GBQ:

QUESTION_1: Identify Where the Data is Stored
In BigQuery, you can identify where data is stored by checking the location of the dataset containing the table. This information can be found in the BigQuery Console by:
1.	Navigating to the dataset where the table resides.
2.	Checking the dataset properties, specifically the location field (e.g., US, EU, etc.).
Alternatively, you can also query the metadata tables available in BigQuery to get storage location information:
SELECT
  dataset_id,
  location
FROM
  `project_id.region-us.INFORMATION_SCHEMA.SCHEMATA`
WHERE
  schema_name = 'your_dataset';

QUESTION_2: View Table Partitions
SELECT
  table_name,
  partitioning_column,
  partitioning_type
FROM
  `project_id.dataset.INFORMATION_SCHEMA.TABLES`
WHERE
  table_name = 'auctions';

QUERY_1: Distribution of Auctions and Line Items Grouped by Number of Segments
SELECT
  ARRAY_LENGTH(arysegments) AS segment_count,
  COUNT(auctionid) AS num_auctions,
  COUNT(DISTINCT idlineitem) AS num_lineitems
FROM
  `your_project.your_dataset.auctions`
GROUP BY
  segment_count
ORDER BY
  segment_count;

QUERY_2: Distinct Count of Auctions and Line Items Associated with Each Segment in arysegments
SELECT
  segment,
  COUNT(DISTINCT auctionid) AS distinct_auctions,
  COUNT(DISTINCT idlineitem) AS distinct_lineitems
FROM
  `your_project.your_dataset.auctions`,
  UNNEST(arysegments) AS segment
GROUP BY
  segment
ORDER BY
  segment;












Linux, Bash

#!/bin/bash

# Set output file and clear any previous content
OUTPUT_FILE="query_results.csv"
> "$OUTPUT_FILE"

# Get the first and last date of the previous month
FIRST_DATE=$(date -d "$(date +%Y-%m-01) -1 month" +%Y-%m-01)
LAST_DATE=$(date -d "$(date +%Y-%m-01) -1 day" +%Y-%m-%d)

# Loop through each date from first to last date of the previous month
CURRENT_DATE="$FIRST_DATE"
while [[ "$CURRENT_DATE" <= "$LAST_DATE" ]]; do
    # SQL query with current date
    SQL_QUERY="select utc_date, sum(1) as num_rows from my_table where utc_date = '${CURRENT_DATE}' group by utc_date"
    mysql -e "$SQL_QUERY" >> "$OUTPUT_FILE"

    # Move to the next date
    CURRENT_DATE=$(date -I -d "$CURRENT_DATE + 1 day")
done

echo "Query results saved to $OUTPUT_FILE"






Python

import subprocess
import datetime
import csv

# Output file to store the query results
output_file = "query_results.csv"

# Get the first and last date of the previous month
today = datetime.date.today()
first_day_of_this_month = today.replace(day=1)
last_day_of_last_month = first_day_of_this_month - datetime.timedelta(days=1)
first_day_of_last_month = last_day_of_last_month.replace(day=1)

# Generate all dates from the previous month
date_range = [first_day_of_last_month + datetime.timedelta(days=i) 
              for i in range((last_day_of_last_month - first_day_of_last_month).days + 1)]

# Open the output file in write mode and create a CSV writer
with open(output_file, mode='w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    # Write the header row
    writer.writerow(['utc_date', 'num_rows'])
    
    # Loop through each date in the range
    for date in date_range:
        date_str = date.strftime('%Y-%m-%d')
        # Define the query for the current date
        query = f"select utc_date, sum(1) as num_rows from my_table where utc_date = '{date_str}' group by utc_date"
        
        # Execute the Hive query
        result = subprocess.run(['hive', '-e', query], capture_output=True, text=True)
        
        # Check if the query was successful
        if result.returncode == 0:
            # Parse the query result and write to CSV
            for line in result.stdout.strip().split('\n'):
                if line:  # Ensure line is not empty
                    # Assume the result format is 'utc_date\tnum_rows' and split by tab
                    row = line.split('\t')
                    writer.writerow(row)
        else:
            # Log an error message if the query fails
            print(f"Error executing query for date {date_str}: {result.stderr}")

print(f"Query results saved to {output_file}")
