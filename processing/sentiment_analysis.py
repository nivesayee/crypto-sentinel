import sqlite3
import pandas as pd
from integration.api.config import db_name, table_name
from textblob import TextBlob

# Connect to SQLite DB
conn = sqlite3.connect(db_name)
query = f"SELECT * FROM {table_name}"
df = pd.read_sql_query(query, conn)


# Perform Sentiment Analysis
def perform_sentiment_analysis(text):
    """
    Performs sentiment analysis on the text
    :param text: Post Content
    :return: Sentiment (positive, negative or neutral)
    """
    blob = TextBlob(text)
    if blob.sentiment.polarity > 0:
        return 'positive'
    elif blob.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'


df["sentiment"] = df["content"].apply(perform_sentiment_analysis)

# Write Updated Dataframe back to SQLite db
df.to_sql(table_name, conn, if_exists="replace", index=False)


# Commit transaction and Close connection
conn.commit()
conn.close()
