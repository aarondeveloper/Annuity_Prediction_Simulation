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
    s3_bucket = args['S3_BUCKET_NAME']
    iam_role_arn = args['REDSHIFT_IAM_ROLE_ARN']
    redshift_connection_name = "Redshift connection" # The name of the connection you created
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
        
        # Convert to DynamicFrame for Glue
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, "annuity_data")
        
        # Write to Redshift using the pre-defined Glue Connection
        print(f"Writing data to Redshift using connection: {redshift_connection_name}")
        
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="redshift",
            connection_options={
                "connectionName": redshift_connection_name,
                "dbtable": redshift_table,
                "redshiftTmpDir": s3_temp_dir,
                "aws_iam_role": iam_role_arn,
                "preactions": f"DROP TABLE IF EXISTS {redshift_table}; CREATE TABLE {redshift_table} (policy_id VARCHAR, issue_age INT, issue_date VARCHAR, premium BIGINT, has_gmwb BOOLEAN, has_gmib BOOLEAN, gmwb_rider_fee FLOAT, gmib_rider_fee FLOAT, death_benefit_type VARCHAR, death_benefit_fee FLOAT, has_beneficiary BOOLEAN, me_fees FLOAT, admin_fees FLOAT, rider_fees FLOAT, total_fees FLOAT);",
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