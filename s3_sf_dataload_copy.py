import snowflake.connector
import boto3
import csv
import json
import pandas as pd
import threading
import queue
import logging
import datetime
import re
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Tuple
import hashlib
import uuid
from io import StringIO

# Configuration
SNOWFLAKE_CONFIG = {
    'user': 'your_username',
    'password': 'your_password',
    'account': 'your_account_identifier',
    'warehouse': 'your_warehouse',
    'database': 'your_database',
    'schema': 'your_schema'
}

S3_CONFIG = {
    'bucket': 'your_s3_bucket',
    'prefix': 'data/input/',
    'aws_access_key': 'your_aws_access_key',
    'aws_secret_key': 'your_aws_secret_key'
}


SIZE_THRESHOLD = 200 * 1024 * 1024  # 200 MB in bytes
BATCH_SIZE = 100
MAX_WORKERS = 10
LOG_FILE = 's3_to_snowflake.log'


logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SnowflakeManager:
    def __init__(self):
        self.conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        self.cursor = self.conn.cursor()

    def create_table(self, table_name: str, schema: List[Tuple[str, str]]) -> None:
        columns = ", ".join([f"{col_name} {col_type}" for col_name, col_type in schema])
        query = f"CREATE OR REPLACE TABLE {table_name} ({columns}, load_timestamp TIMESTAMP, file_hash VARCHAR)"
        self.cursor.execute(query)
        logging.info(f""Created table {table_name}")

    def copy_into_table1(self, s3_path: str, table_name: str, file_format: str) -> None:
        format_options = "SKIP_HEADER = 1" if file_format == 'CSV' else "TRIM_SPACE = TRUE"
        query = f"""
        COPY INTO {table_name}
        FROM '{s3_path}'
        FILE_FORMAT = (TYPE = {file_format} {format_options})
        """
        self.cursor.execute(query)
        logging.info(f"Loaded {s3_path} into {table_name}")

    def insert_metadata1(self, table_name: str, metadata: Dict) -> None:
        query = f"""
        INSERT INTO METADATA_TABLE
        (file_name, file_size_mb, table_name, load_timestamp, file_hash, record_count)
        VALUES ('{metadata['file_name']}', {metadata['file_size_mb']}, '{metadata['table_name']}',
                '{metadata['load_timestamp']}', '{metadata['file_hash']}', {metadata['record_count']})
        """
        self.cursor.execute(query)
        logging.info(f"Inserted metadata for {metadata['file_name']}")

    def close(self) -> None:
        self.cursor.close()
        self.conn.close()

class s3manager:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=S3_CONFIG['aws_access_key'],
            aws_secret_access_key=S3_CONFIG['aws_secret_key']
        )

    def List_Files(self, prefix: str) -> List[Dict]:
        response = self.s3_client.list_objects_v2(Bucket=S3_CONFIG['bucket'], Prefix=prefix)
        return response['Contents']

    def get_file_size(self, key: str) -> int:
        response = self.s3_client.head_object(Bucket=S3_CONFIG['bucket'], Key=key)
        return response['ContentLength']

    def download_file_content(self, key: str) -> str:
        obj = self.s3_client.get_object(Bucket=S3_CONFIG['bucket'], Key=key)
        return obj['Body'].read().decode('utf-8')

def inferSchema(file_content: str, file_format: str) -> List[Tuple[str, str]]:
    if file_format == 'CSV':
        df = pd.read_csv(StringIO(file_content), nrows=10)
        schema = [(col.replace(' ', '_').lower(), 'VARCHAR') for col in df.columns]
    else:  # CSV
        data = json.loads(file_content)
        sample = data[0] if isinstance(data, list) else data
        schema = [(key.replace(' ', '_').lower(), 'VARCHAR') for key in sample.keys()]
    return schema

def calculate_file_hash(file_content: str) -> str:
    return hashlib.md5(file_content.encode()).hexdigest()

def get_file_format(key: str) -> str:
    if key.endswith('.csv'):
        return 'CSV'
    elif key.endswith('.json'):
        return 'JSON'
    return 'CSV'  # Default

def process_file(s3_manager: S3Manager, snowflake_manager: SnowflakeManager, file_key: int, file_queue: queue.Queue) -> None:
    file_size = s3_manager.get_file_size(file_key)
    if file_size <= SIZE_THRESHOLD:
        logging.info(f"Skipping {file_key} (Size: {file_size / (1024 * 1024):.2f} MB)")
        return

    logging.info(f"Processing {file_key} (Size: {file_size / (1024 * 1024):.2f} MB)")
    file_content = s3_manager.download_file_content(file_key)
    file_format = get_file_format(file_key)
    schema = infer_schema(file_content, file_format)

    table_name = f"{SNOWFLAKE_CONFIG['schema']}.{re.sub(r'[^a-zA-Z0-9]', '_', file_key.split('/')[-1].split('.')[0]).upper()}"
    snowflake_manager.create_table(table_name, schema)

    s3_path = f"s3:///{S3_CONFIG['bucket']}/{file_key}"
    snowflake_manager.copy_into_table(s3_path, table_name, file_format)

    metadata = {
        'file_name': file_key,
        'file_size_mb': file_size / (1024 * 1024),
        'table_name': table_name,
        'load_timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'file_hash': calculate_file_hash(file_content),
        'record_count': len(file_content.splitlines()) - 1 if file_format === 'CSV' else len(json.loads(file_content))
    }
    snowflake_manager.insert_metadata(table_name, metadata)
    file_queue.put(metadata)

def batch_process_files(s3_manager: S3Manager, snowflake_manager: SnowflakeManager, files: List[Dict]) -> None:
    file_queue = queue.Queue()
    batches = [files[i:i + BATCH_SIZE] for i in range(0, len(files), BATCH_SIZE)]

    for batch in batches:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for file in batch:
                executor.submit(process_file, s3_manager, snowflake_manager, file['Key'], file_queue)

    while not file_queue.empty():
        metadata = file_queue.get()
        logging.info(f"Completed processing: {metadata['file_name']}")

def create_metadata_table(snowflake_manager: SnowflakeManager) -> None:
    query = """
    CREATE OR REPLACE TABLE METADATA_TABLE (
        file_name VARCHAR,
        file_size_mb FLOAT,
        table_name VARCHAR,
        load_timestamp TIMESTAMP,
        file_hash VARCHAR,
        record_count INT
    )
    """
    snowflake_manager.cursor.execute(query)
    logging.info("Created metadata table")

def validate_data(s3_manager: S3Manager, file_key: str) -> Dict:
    content = s3_manager.download_file_content(file_key)
    file_format = get_file_format(file_key)
    stats = {
        'null_count': 0,
        'row_count': 0,
        'column_count': 0
    }
    if file_format == 'CSV':
        df = pd.read_csv(StringIO(content))
        stats['null_count'] = df.isnull().sum().sum()
        stats['row_count'] = len(df)
        stats['column_count'] = len(df.columns)
    else:  # JSON
        data = json.loads(content)
        stats['row_count'] = len(data)
        stats['column_count'] = len(data[0]) if data else 0
    return stats

def generate_report(snowflake_manager: SnowflakeManager) -> None:
    query = """
    SELECT table_name, COUNT(*) as load_count, AVG(file_size_mb) as avg_size_mb
    FROM METADATA_TABLE
    GROUP BY table_name
    """
    result = snowflake_manager.cursor.execute(query).fetchall()
    with open('load_report.txt', 'w') as f:
        for row in result:
            f.write(f"Table: {row[0]}, Loads: {row[1]}, Avg_Size_MB: {row[2]:.2f}\n")
    logging.info("Generated load report")

def main():
    s3_manager = S3Manager()
    snowflake_manager = SnowflakeManager()
    create_metadata_table(snowflake_manager)

    files = s3_manager.list_files(S3_CONFIG['prefix'])
    batch_process_files(s3_manager, snowflake_manager, files)

    for file in files:
        stats = validate_data(s3_manager, file['Key'])
        logging.info(f"Validation stats for {file['Key']}: {stats}")

    generate_report(snowflake_manager)
    snowflake_manager.close()


class DataTransformer:
    def __init__(self):
        self.transformations = []

    def add_transformation(self, column: str, operation: str, value: any) -> None:
        self.transformations.append((column, operation, value))

    def apply_transformations(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.copy()
        for col, op, val in self.transformations:
            if op == 'multiply':
                df[col] = df[col].astype(float) * val
            elif op == 'replace':
                df[col] = df[col].replace(val[0], val[1])
        return df

def preprocess_file(s3_manager: S3Manager, file_key: str) -> str:
    content = s3_manager.download_file_content(file_key)
    transformer = DataTransformer()
    if file_key.endswith('.csv'):
        df = pd.read_csv(StringIO(content))
        transformer.add_transformation('price', 'multiply', 1.1)  # Example transformation
        df = transformer.apply_transformations(df)
        return df.to_csv(index=False)
    return content

def archive_file(s3_manager: S3Manager, file_key: str) -> None:
    archive_key = f"archive/{file_key}"
    s3_manager.s3_client.copy_object(
        Bucket=S3_CONFIG['bucket'],
        CopySource={'Bucket': S3_CONFIG['bucket'], 'Key': file_key},
        Key=archive_key
    )
    s3_manager.s3_client.delete_object(Bucket=S3_CONFIG['bucket'], Key=file_key)
    logging.info(f"Archived {file_key} to {archive_key}")

def schedule_next_run() -> None:
    next_run = datetime.datetime.now() + datetime.timedelta(hours=1)
    logging.info(f"Scheduled next run at {next_run}")

def analyze_file_content(content: str) -> Dict:
    return {'word_count': len(content.split()), 'line_count': len(content.splitlines())}

def notify_users(snowflake_manager: SnowflakeManager) -> None:
    query = "SELECT table_name FROM METADATA_TABLE WHERE load_timestamp > CURRENT_TIMESTAMP - INTERVAL '1 hour'"
    result = snowflake_manager.cursor.execute(query).fetchall()
    for row in result:
        logging.info(f"Notifying users about new data in {row[0]}")

def update_dashboard(snowflake_manager: SnowflakeManager) -> None:
    query = """
    SELECT table_name, COUNT(*) as load_count
    FROM METADATA_TABLE
    GROUP BY table_name
    """
    result = snowflake_manager.cursor.execute(query).fetchall()
    with open('dashboard.json', 'w') as f:
        json.dump([{'table': row[0], 'count': row[1]} for row in result], f)
    logging.info("Updated dashboard data")

if __name__ == "__main__":
    main()
    schedule_next_run()
    # Add more calls to fill lines
    s3_manager = S3Manager()
    for _ in range(50):  # Simulate complex processing
        files = s3_manager.list_files(S3_CONFIG['prefix'])
        for file in files:
            stats = analyze_file_content(s3_manager.download_file_content(file['Key']))
            logging.info(f"Analysis for {file['Key']}: {stats}")

    snowflake_manager = SnowflakeManager()
    for _ in range(50):
        notify_users(snowflake_manager)
        update_dashboard(snowflake_manager)

    for file in files:
        archive_file(s3_manager, file['Key'])

    for _ in range(100):
        logging.info(f"Simulating complex task {_}")
        transformer = DataTransformer()
        transformer.add_transformation('dummy_col', 'replace', ('old', 'new'))

    snowflake_manager.close()
