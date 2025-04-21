import os

import psycopg2
from dotenv import load_dotenv
from psycopg2 import OperationalError

# Load environment variables from .env file
load_dotenv()

print(os.environ.get("DB_NAME"))
print(os.environ.get("DB_USER"))
print(os.environ.get("DB_PASSWORD"))
print(os.environ.get("DB_HOST"))
print(os.environ.get("DB_PORT"))

try:
    conn_params = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
    }

    # Connect to the database
    conn = psycopg2.connect(**conn_params)

    # Create a cursor object
    cur = conn.cursor()

    # Execute a simple query to test the connection
    cur.execute("SELECT 1;")
    result = cur.fetchone()

    print("Connection successful, query result:", result)

    # Close the cursor and connection
    cur.close()
    conn.close()

except OperationalError as e:
    print("Error while connecting to PostgreSQL:", e)
