import snowflake.connector
import boto3
import os
from botocore.exceptions import ClientError

# Snowflake connection parameters
SNOWFLAKE_CONFIG = {
    'user': 'your_username',
    'password': 'your_password',
    'account': 'your_account_identifier',
    'warehouse': 'your_warehouse',
    'database': 'your_database',
    'schema': 'your_schema'
}

# AWS S3 parameters
S3_BUCKET = 'your_s3_bucket'
S3_PREFIX = 'path/to/files/'  # Optional: specify a folder in the bucket
AWS_ACCESS_KEY = 'your_aws_access_key'
AWS_SECRET_KEY = 'your_aws_secret_key'

# File size threshold (200 MB in bytes)
SIZE_THRESHOLD = 200 * 1024 * 1024  # 200 MB

def connect_to_snowflake():
    """Establish connection to Snowflake."""
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        raise

def get_s3_client():
    """Create an S3 client."""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        return s3_client
    except Exception as e:
        print(f"Error creating S3 client: {e}")
        raise

def check_file_size(s3_client, bucket, key):
    """Check if the file size is greater than the threshold."""
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        file_size = response['ContentLength']
        return file_size > SIZE_THRESHOLD, file_size
    except ClientError as e:
        print(f"Error checking file {key}: {e}")
        return False, 0

def load_file_to_snowflake(conn, s3_path, table_name):
    """Load data from S3 to Snowflake using COPY INTO."""
    try:
        cursor = conn.cursor()
        copy_query = f"""
        COPY INTO {table_name}
        FROM '{s3_path}'
        FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
        """
        cursor.execute(copy_query)
        print(f"Successfully loaded {s3_path} into {table_name}")
        cursor.close()
    except Exception as e:
        print(f"Error loading {s3_path} to Snowflake: {e}")
        raise

def main():
    """Main function to check S3 files and load to Snowflake."""
    # Initialize connections
    snowflake_conn = connect_to_snowflake()
    s3_client = get_s3_client()

    try:
        # List objects in S3 bucket
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
        if 'Contents' not in response:
            print("No files found in the specified S3 path.")
            return

        # Process each file
        for obj in response['Contents']:
            file_key = obj['Key']
            is_large, file_size = check_file_size(s3_client, S3_BUCKET, file_key)
            
            if is_large:
                print(f"File {file_key} ({file_size / (1024 * 1024):.2f} MB) exceeds 200 MB. Loading to Snowflake...")
                s3_path = f"s3://{S3_BUCKET}/{file_key}"
                table_name = 'YOUR_TARGET_TABLE'  # Specify your Snowflake table
                load_file_to_snowflake(snowflake_conn, s3_path, table_name)
            else:
                print(f"File {file_key} ({file_size / (1024 * 1024):.2f} MB) is under 200 MB. Skipping...")

    except Exception as e:
        print(f"Error in main process: {e}")
    finally:
        snowflake_conn.close()

if __name__ == "__main__":
    main()
