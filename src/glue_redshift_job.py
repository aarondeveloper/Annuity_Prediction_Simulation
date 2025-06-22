import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'REDSHIFT_HOST',
    'REDSHIFT_PORT',
    'REDSHIFT_DB',
    'REDSHIFT_USER',
    'REDSHIFT_PASSWORD',
    'S3_BUCKET_NAME',
    'REDSHIFT_IAM_ROLE_ARN'
])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def main():
    """Main ETL job to load annuity data into Redshift."""
    
    # Configuration from Glue Job Parameters
    redshift_host = args['REDSHIFT_HOST']
    redshift_port = args['REDSHIFT_PORT']
    redshift_db = args['REDSHIFT_DB']
    redshift_user = args['REDSHIFT_USER']
    redshift_password = args['REDSHIFT_PASSWORD']
    s3_bucket = args['S3_BUCKET_NAME']
    iam_role_arn = args['REDSHIFT_IAM_ROLE_ARN']
    
    redshift_table = 'annuity_policies'
    
    # S3 paths
    s3_data_path = f"s3://{s3_bucket}/data/annuity_policies.csv"
    s3_temp_dir = f"s3://{s3_bucket}/temp/"
    
    print(f"Starting ETL job to load data into Redshift table: {redshift_table}")
    print(f"Reading source data from: {s3_data_path}")
    
    try:
        # Read the CSV file from S3 using Spark
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_data_path)
        
        print(f"Successfully read {df.count()} records from CSV")
        print("Data schema:")
        df.printSchema()
        
        # Convert to DynamicFrame for Glue
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, "annuity_data")
        
        # Write to Redshift using Glue's native Redshift connector
        print("Writing data to Redshift...")
        
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="redshift",
            connection_options={
                "url": f"jdbc:redshift://{redshift_host}:{redshift_port}/{redshift_db}",
                "dbtable": redshift_table,
                "user": redshift_user,
                "password": redshift_password,
                "redshiftTmpDir": s3_temp_dir,
                "aws_iam_role": iam_role_arn,
                "preactions": f"DROP TABLE IF EXISTS {redshift_table}; CREATE TABLE {redshift_table} (policy_id VARCHAR, issue_age INT, issue_date VARCHAR, premium BIGINT, has_gmwb BOOLEAN, has_gmib BOOLEAN, gmwb_rider_fee FLOAT, gmib_rider_fee FLOAT, death_benefit_type VARCHAR, death_benefit_fee FLOAT, has_beneficiary BOOLEAN, me_fees FLOAT, admin_fees FLOAT, rider_fees FLOAT, total_fees FLOAT);",
                "postactions": "COMMIT"
            }
        )
        
        print(f"Successfully loaded {df.count()} records into Redshift table: {redshift_table}")
        
    except Exception as e:
        print(f"Error during ETL job: {str(e)}")
        raise e
    
    finally:
        job.commit()

if __name__ == "__main__":
    main() 