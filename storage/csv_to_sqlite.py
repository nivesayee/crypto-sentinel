import sqlite3
import pandas as pd
from integration.api.config import reddit_posts_file, db_name, table_name

# Read the csv file using pandas
df = pd.read_csv(reddit_posts_file)

# Remove rows with content None
df = df.dropna(subset=["content"])

# Connect to SQLite DB or create one if it doesn't exist
conn = sqlite3.connect(db_name)

# Write the Dataframe to SQLite table
# If the table doesn't exist pandas will create it
df.to_sql(table_name, conn, if_exists="replace", index=False)

# Commit transaction and close the connection
conn.commit()
conn.close()

print(f"Data from {reddit_posts_file} file has been written to table {table_name}")