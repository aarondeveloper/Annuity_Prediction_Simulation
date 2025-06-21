"""
PySpark Script to Load Annuity Data into Amazon Redshift
This script demonstrates how to use PySpark to load CSV data into Redshift programmatically.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os

def create_spark_session():
    """Create and configure Spark session for Redshift connectivity."""
    spark = SparkSession.builder \
        .appName("AnnuityDataToRedshift") \
        .config("spark.jars.packages", 
                "io.github.spark-redshift-community:spark-redshift_2.12:5.0.3,"
                "com.amazonaws:aws-java-sdk-s3:1.12.261,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
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

def load_data_to_redshift(spark, csv_path, redshift_table_name):
    """
    Load CSV data into Redshift using PySpark.
    
    Args:
        spark: SparkSession
        csv_path: Path to the CSV file
        redshift_table_name: Name of the target table in Redshift
    """
    
    # Read CSV file
    print(f"Reading CSV from: {csv_path}")
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    
    print(f"Data loaded: {df.count()} rows")
    print("Schema:")
    df.printSchema()
    
    # Show sample data
    print("\nSample data:")
    df.show(5)
    
    # Redshift connection parameters
    redshift_options = {
        "url": "jdbc:redshift://your-cluster.region.redshift.amazonaws.com:5439/your_database",
        "dbtable": redshift_table_name,
        "user": "your_username",
        "password": "your_password",
        "driver": "com.amazon.redshift.jdbc42.Driver"
    }
    
    # Write to Redshift
    print(f"\nWriting data to Redshift table: {redshift_table_name}")
    
    # Method 1: Direct JDBC write (for smaller datasets)
    df.write \
        .format("jdbc") \
        .options(**redshift_options) \
        .mode("overwrite") \
        .save()
    
    print("Data successfully loaded to Redshift!")

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
    """Main function to orchestrate the data loading process."""
    
    # Initialize Spark
    spark = create_spark_session()
    
    # File paths
    csv_path = "data/annuity_policies.csv"
    redshift_table = "annuity_policies"
    
    try:
        # Load data to Redshift
        load_data_to_redshift(spark, csv_path, redshift_table)
        
        # Optional: Verify data was loaded
        print("\nVerifying data in Redshift...")
        # You could add a verification query here
        
    except Exception as e:
        print(f"Error loading data to Redshift: {str(e)}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 