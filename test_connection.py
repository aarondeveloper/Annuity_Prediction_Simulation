"""
Simple Redshift Connection Test
"""

import os
from dotenv import load_dotenv
import socket

# Load environment variables
load_dotenv()

def test_connection():
    host = os.getenv("REDSHIFT_HOST")
    port = int(os.getenv("REDSHIFT_PORT", 5439))
    
    print(f"Testing connection to: {host}:{port}")
    
    try:
        # Test basic socket connection
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print("✅ Port is reachable!")
        else:
            print("❌ Port is not reachable")
            print(f"Error code: {result}")
            
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
    
    # Print environment variables (without sensitive data)
    print(f"\nHost: {host}")
    print(f"Port: {port}")
    print(f"Database: {os.getenv('REDSHIFT_DB')}")
    print(f"User: {os.getenv('REDSHIFT_USER')}")

if __name__ == "__main__":
    test_connection() 