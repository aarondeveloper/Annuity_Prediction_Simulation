"""
PySpark Script to Load Annuity Data into Amazon Redshift
This script uses the spark-redshift connector to efficiently load data
via an S3 staging area, which is the recommended best practice.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def create_spark_session():
    """Create and configure Spark session for Redshift connectivity."""
    # This configuration tells Spark to download the necessary packages to connect
    # to Redshift and S3. Your local machine must have credentials configured
    # (e.g., via `aws configure`) for S3 access.
    spark = SparkSession.builder \
        .appName("AnnuityDataToRedshift") \
        .config("spark.jars.packages", 
                "io.github.spark-redshift-community:spark-redshift_2.12:5.0.3,"
                "com.amazonaws:aws-java-sdk-s3:1.12.261,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "org.apache.spark:spark-avro_2.12:3.5.1") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
        .getOrCreate()
    
    return spark

def create_annuity_schema():
    """Define the schema for the annuity policies table."""
    schema = StructType([
        StructField("policy_id", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("gender", StringType(), False),
        StructField("start_date", StringType(), False),
        StructField("deferral_years", IntegerType(), False),
        StructField("initial_value", DoubleType(), False),
        StructField("rider_type", StringType(), False),
        StructField("has_death_benefit", BooleanType(), False),
        StructField("death_benefit_type", StringType(), False),
        StructField("has_beneficiary", BooleanType(), False),
        StructField("me_fees", DoubleType(), False),
        StructField("admin_fees", DoubleType(), False),
        StructField("rider_fees", DoubleType(), False),
        StructField("death_benefit_fees", DoubleType(), False),
        StructField("total_fees", DoubleType(), False),
        StructField("status", StringType(), False)
    ])
    return schema

def load_data_to_redshift(spark, csv_path, redshift_table_name, jdbc_url, s3_staging_dir, iam_role_arn):
    """
    Load CSV data into Redshift using the high-performance S3 staging method.
    
    Args:
        spark: SparkSession object
        csv_path (str): Path to the source CSV file.
        redshift_table_name (str): The name of the target table in Redshift.
        jdbc_url (str): The JDBC URL for the Redshift cluster.
        s3_staging_dir (str): The S3 path for staging temporary data.
        iam_role_arn (str): The IAM Role ARN that Redshift will assume to read from S3.
    """
    
    # Read CSV file with inferred schema
    print(f"Reading CSV from: {csv_path}")
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    
    print(f"Data loaded successfully: {df.count()} rows")
    print("Inferred Schema:")
    df.printSchema()
    
    print("\nWriting data to Redshift table:", redshift_table_name)
    print("Using S3 staging directory:", s3_staging_dir)
    
    # Use the spark-redshift connector for an efficient S3-based write.
    # This process writes the DataFrame to a temporary S3 location (`tempdir`)
    # and then executes a Redshift COPY command to load it into the table.
    df.write \
      .format("io.github.spark_redshift_community.spark.redshift") \
      .option("url", jdbc_url) \
      .option("dbtable", redshift_table_name) \
      .option("tempdir", s3_staging_dir) \
      .option("aws_iam_role", iam_role_arn) \
      .option("user", os.getenv("REDSHIFT_USER")) \
      .option("password", os.getenv("REDSHIFT_PASSWORD")) \
      .mode("overwrite") \
      .save()
    
    print("\nData successfully loaded to Redshift!")

def create_redshift_table_sql():
    """SQL to create the annuity policies table in Redshift."""
    sql = """
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
    return sql

def main():
    """Main function to configure and orchestrate the data loading process."""
    
    # --- Load Configuration from .env file ---
    host = os.getenv("REDSHIFT_HOST")
    port = os.getenv("REDSHIFT_PORT")
    db = os.getenv("REDSHIFT_DB")
    user = os.getenv("REDSHIFT_USER")
    password = os.getenv("REDSHIFT_PASSWORD")
    s3_bucket = os.getenv("S3_BUCKET_NAME")
    iam_role_arn = os.getenv("REDSHIFT_IAM_ROLE_ARN")

    # --- Validate Configuration ---
    if not all([host, port, db, user, password, s3_bucket, iam_role_arn]):
        print("Error: One or more required environment variables are not set.")
        print("Please check your .env file and ensure all variables are filled in.")
        return

    # --- Construct URLs and Paths ---
    jdbc_url = f"jdbc:redshift://{host}:{port}/{db}"
    s3_staging_dir = f"s3a://{s3_bucket}/temp/redshift-staging/"
    
    # --- File Paths ---
    csv_path = "data/annuity_policies.csv"
    redshift_table = "annuity_policies"
    
    spark = None
    try:
        # Initialize Spark
        spark = create_spark_session()
        
        # Note: This script assumes the target Redshift table `annuity_policies`
        # has been created beforehand with the correct schema.
        # You can use a separate SQL script or a tool like `psycopg2` to create it.
        print("Starting Redshift data load process...")
        load_data_to_redshift(spark, csv_path, redshift_table, jdbc_url, s3_staging_dir, iam_role_arn)
        
    except Exception as e:
        print(f"\nAn error occurred during the data loading process: {str(e)}")
        raise
    
    finally:
        if spark:
            print("Stopping Spark session.")
            spark.stop()

if __name__ == "__main__":
    main() 