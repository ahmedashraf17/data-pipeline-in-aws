'''this code setup a data pipeline that fetch the data from mysql database stage the data in s3 ,
   transform it in aws glue and load the transformed data into redshift and schedule the pipeline with glue scheduler'''

#MySQL Data Extraction:

import mysql.connector

# Establish connection with MySQL database
connection = mysql.connector.connect(
    host='your-mysql-hostname',
    user='your-mysql-username',
    password='your-mysql-password',
    database='your-mysql-database'
)

# Execute SQL query to extract data
cursor = connection.cursor()
query = 'SELECT * FROM your_table'
cursor.execute(query)

# Fetch and transform data
data = cursor.fetchall()
transformed_data = transform_function(data)

# Close database connection
cursor.close()
connection.close()

#data staging in s3

import boto3

# Create an S3 client
s3 = boto3.client('s3')

# Upload data to S3 bucket
bucket_name = 'your-s3-bucket'
key = 'your-key-prefix/data.json'
s3.put_object(Body=json.dumps(transformed_data), Bucket=bucket_name, Key=key)

#loading the data int redshift
import boto3

# Create a Redshift client
redshift = boto3.client('redshift')

# Load transformed data into Redshift
cluster_identifier = 'your-redshift-cluster-identifier'
database_name = 'your-database-name'
table_name = 'your-table-name'
s3_uri = f's3://{bucket_name}/{key}'

copy_query = f"""
    COPY {database_name}.{table_name}
    FROM '{s3_uri}'
    ACCESS_KEY_ID 'your-access-key-id'
    SECRET_ACCESS_KEY 'your-secret-access-key'
    REGION 'your-aws-region'
    JSON 'auto';
"""

response = redshift.execute_statement(
    ClusterIdentifier=cluster_identifier,
    Database=database_name,
    DbUser='your-db-username',
    Sql=copy_query
)

if response['ResponseMetadata']['HTTPStatusCode'] == 200:
    print('Data loaded into Redshift successfully')
else:
    print('Error loading data into Redshift')
