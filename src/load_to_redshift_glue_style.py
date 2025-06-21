"""
Alternative Approach: S3 + Redshift COPY (AWS Glue Style)
This approach uploads data to S3 first, then uses Redshift COPY command.
Better for large datasets and follows AWS best practices.
"""

import boto3
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def upload_to_s3(csv_path, bucket_name, s3_key):
    """
    Upload CSV file to S3 bucket.
    
    Args:
        csv_path: Local path to CSV file
        bucket_name: S3 bucket name
        s3_key: S3 object key (file path in bucket)
    """
    s3_client = boto3.client('s3')
    
    print(f"Uploading {csv_path} to s3://{bucket_name}/{s3_key}")
    
    try:
        s3_client.upload_file(csv_path, bucket_name, s3_key)
        print("Upload successful!")
        return f"s3://{bucket_name}/{s3_key}"
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        raise

def create_redshift_table(connection_params):
    """
    Create the annuity policies table in Redshift.
    
    Args:
        connection_params: Dictionary with Redshift connection details
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS annuity_policies (
        policy_id VARCHAR(50) PRIMARY KEY,
        age INTEGER NOT NULL,
        gender VARCHAR(1) NOT NULL,
        start_date VARCHAR(10) NOT NULL,
        deferral_years INTEGER NOT NULL,
        initial_value DECIMAL(15,2) NOT NULL,
        rider_type VARCHAR(20) NOT NULL,
        has_death_benefit BOOLEAN NOT NULL,
        death_benefit_type VARCHAR(20) NOT NULL,
        has_beneficiary BOOLEAN NOT NULL,
        me_fees DECIMAL(5,4) NOT NULL,
        admin_fees DECIMAL(5,4) NOT NULL,
        rider_fees DECIMAL(5,4) NOT NULL,
        death_benefit_fees DECIMAL(5,4) NOT NULL,
        total_fees DECIMAL(5,4) NOT NULL,
        status VARCHAR(20) NOT NULL
    )
    DISTKEY (rider_type)
    SORTKEY (age, start_date);
    """
    
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    
    try:
        cursor.execute(create_table_sql)
        conn.commit()
        print("Table created successfully!")
    except Exception as e:
        print(f"Error creating table: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

def copy_data_from_s3(connection_params, s3_path, iam_role):
    """
    Use Redshift COPY command to load data from S3.
    
    Args:
        connection_params: Redshift connection details
        s3_path: S3 path to the CSV file
        iam_role: IAM role ARN with S3 read permissions
    """
    copy_sql = f"""
    COPY annuity_policies
    FROM '{s3_path}'
    IAM_ROLE '{iam_role}'
    CSV
    IGNOREHEADER 1
    DATEFORMAT 'auto'
    TRUNCATECOLUMNS;
    """
    
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    
    try:
        print("Executing COPY command...")
        cursor.execute(copy_sql)
        conn.commit()
        print("COPY command completed successfully!")
        
        # Verify the data
        cursor.execute("SELECT COUNT(*) FROM annuity_policies;")
        count = cursor.fetchone()[0]
        print(f"Total rows loaded: {count}")
        
    except Exception as e:
        print(f"Error during COPY: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

def main():
    """Main function for S3 + Redshift COPY approach."""
    
    # --- Configuration from environment variables ---
    csv_path = "data/annuity_policies.csv"
    bucket_name = os.getenv("S3_BUCKET_NAME")
    s3_key = "raw/annuity_policies.csv"
    
    # Redshift connection parameters
    redshift_params = {
        'host': os.getenv("REDSHIFT_HOST"),
        'port': os.getenv("REDSHIFT_PORT", 5439), # Default port if not set
        'database': os.getenv("REDSHIFT_DB"),
        'user': os.getenv("REDSHIFT_USER"),
        'password': os.getenv("REDSHIFT_PASSWORD")
    }
    
    # IAM role ARN (needs S3 read permissions)
    iam_role_arn = os.getenv("REDSHIFT_IAM_ROLE_ARN")

    # Validate that all required environment variables are set
    if not all([bucket_name, redshift_params['host'], iam_role_arn]):
        print("Error: Required environment variables (S3_BUCKET_NAME, REDSHIFT_HOST, REDSHIFT_IAM_ROLE_ARN) are not set.")
        print("Please create a .env file based on .env.example and fill in the values.")
        return

    try:
        # Step 1: Upload to S3
        s3_path = upload_to_s3(csv_path, bucket_name, s3_key)
        
        # Step 2: Create table in Redshift
        create_redshift_table(redshift_params)
        
        # Step 3: Copy data from S3 to Redshift
        copy_data_from_s3(redshift_params, s3_path, iam_role_arn)
        
        print("Data loading process completed successfully!")
        
    except Exception as e:
        print(f"Error in data loading process: {str(e)}")
        raise

if __name__ == "__main__":
    main() 