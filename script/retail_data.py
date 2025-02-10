import os
import time
from datetime import datetime

import pandas as pd
from pyathena import connect

start_time = datetime.now()

# Read AWS credentials from environment variables (populate these in GitHub Secrets)
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
region_name = os.environ.get('AWS_REGION', 'ap-south-1')

# Connect to Athena
conn = connect(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    s3_staging_dir='s3://aws-athena-query-results-568130295144-ap-south-1/external-queries-results',
    region_name=region_name,
    schema_name='default'
)

# Read your SQL file from the repo
with open('queries/query.sql', 'r') as file:
    sql_query = file.read()

# Execute the query and load into a DataFrame
df = pd.read_sql(sql_query, conn)

# Save the DataFrame to a CSV
output_file = 'out.csv'
df.to_parquet('output/out.parquet', index=False)

end_time = datetime.now() - start_time
print(f"Query results saved to '{output_file}'")
print(f"Execution time: {end_time}")
