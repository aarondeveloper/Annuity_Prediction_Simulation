"""
Script to Extract Data from Amazon Redshift
This script connects to Redshift and extracts annuity policy data into a Pandas DataFrame.
"""

import pandas as pd
import redshift_connector
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def extract_data_from_redshift(limit=None):
    """
    Extract data from Redshift table into a Pandas DataFrame.
    
    Args:
        limit (int, optional): Limit the number of rows to extract.
    
    Returns:
        pandas.DataFrame: DataFrame containing the extracted data.
    """
    
    # --- Load Configuration from .env file ---
    host = os.getenv("REDSHIFT_HOST")
    port = os.getenv("REDSHIFT_PORT")
    db = os.getenv("REDSHIFT_DB")
    user = os.getenv("REDSHIFT_USER")
    password = os.getenv("REDSHIFT_PASSWORD")

    # --- Validate Configuration ---
    if not all([host, port, db, user, password]):
        print("Error: One or more required environment variables are not set.")
        print("Please check your .env file and ensure all variables are filled in.")
        return None

    # --- Build query with optional limit ---
    query = "SELECT * FROM annuity_policies"
    if limit:
        query += f" LIMIT {limit}"
    
    print(f"Extracting data from Redshift table: annuity_policies")
    print(f"Query: {query}")
    print(f"Connecting to: {host}:{port}/{db}")
    
    try:
        # Connect to Redshift using the official AWS connector
        conn = redshift_connector.connect(
            host=host,
            database=db,
            port=int(port),
            user=user,
            password=password
        )
        
        # Create a cursor
        cursor = conn.cursor()
        
        # Execute query
        cursor.execute(query)
        
        # Fetch all results
        results = cursor.fetchall()
        
        # Get column names from cursor description
        columns = [desc[0] for desc in cursor.description]
        
        # Create DataFrame
        df = pd.DataFrame(results, columns=columns)
        
        print(f"Successfully extracted {len(df)} rows from Redshift!")
        print(f"DataFrame shape: {df.shape}")
        print("DataFrame columns:", list(df.columns))
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        return df
        
    except Exception as e:
        print(f"\nAn error occurred during data extraction: {str(e)}")
        print(f"Connection details: {host}:{port}/{db}")
        raise

def main():
    """Main function to extract data from Redshift."""
    return extract_data_from_redshift()

if __name__ == "__main__":
    df = main()
    if df is not None:
        print("\nFirst few rows of extracted data:")
        print(df.head()) 