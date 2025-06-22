import boto3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def upload_csv_to_s3():
    """Upload the annuity policies CSV to S3 for Glue job processing."""
    
    # Get configuration from environment
    s3_bucket = os.getenv('S3_BUCKET_NAME')
    csv_file_path = 'data/annuity_policies.csv'
    s3_key = 'data/annuity_policies.csv'
    
    if not s3_bucket:
        raise ValueError("S3_BUCKET_NAME not found in environment variables")
    
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"CSV file not found at {csv_file_path}")
    
    try:
        # Create S3 client
        s3_client = boto3.client('s3')
        
        print(f"Uploading {csv_file_path} to s3://{s3_bucket}/{s3_key}")
        
        # Upload the file
        s3_client.upload_file(
            csv_file_path,
            s3_bucket,
            s3_key,
            ExtraArgs={'ContentType': 'text/csv'}
        )
        
        print(f"Successfully uploaded CSV to s3://{s3_bucket}/{s3_key}")
        
        # Create the staging and temp directories (S3 doesn't need actual directories, but it's good practice)
        s3_client.put_object(Bucket=s3_bucket, Key='staging/')
        s3_client.put_object(Bucket=s3_bucket, Key='temp/')
        
        print("Created staging and temp directories in S3")
        
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        raise e

if __name__ == "__main__":
    upload_csv_to_s3() 