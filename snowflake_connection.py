import os
import snowflake.connector

# Read credentials from environment variables
USER = os.getenv("SNOWSQL_USER")
PASSWORD = os.getenv("SNOWSQL_PWD")
ACCOUNT = os.getenv("SNOWSQL_ACCOUNT")

# Establish connection
conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT
)

# Check connection
cur = conn.cursor()
cur.execute("SELECT CURRENT_VERSION()")  # Get Snowflake version
print(f"Snowflake Version: {cur.fetchone()[0]}")

# Close connection
cur.close()
conn.close()
