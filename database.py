"""
This module handles all database interactions for the litanai project,
including connections to ClickHouse and SQLite.
"""

import clickhouse_connect
import sqlite3
import pandas as pd
import ibis

def get_clickhouse_client(host='localhost', port=8123, database="litanai"):
    """Establishes a connection to the ClickHouse database."""
    try:
        return clickhouse_connect.get_client(host=host, port=port, database=database)
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}")
        return None

def get_ibis_clickhouse_connection(database="litanai"):
    """Establishes an Ibis connection to the ClickHouse database."""
    try:
        return ibis.connect(f"clickhouse://localhost/{database}")
    except Exception as e:
        print(f"Error connecting to Ibis ClickHouse: {e}")
        return None

def get_ibis_sqlite_connection(db_name='openai_responses.db'):
    """Establishes an Ibis connection to the SQLite database."""
    try:
        return ibis.connect(f"sqlite://{db_name}")
    except Exception as e:
        print(f"Error connecting to Ibis SQLite: {e}")
        return None

def create_littext_table(client):
    """
    Creates the littext table in ClickHouse with an inverted index.
    Drops the existing table if it exists.
    """
    print("Creating 'littext' table...")
    client.command("SET allow_experimental_full_text_index = true;")
    client.command("DROP TABLE IF EXISTS littext")
    client.command("SET allow_experimental_inverted_index = true;")
    client.command("""
    CREATE TABLE littext (
        `key` String,
    
        `text` String,
        INDEX inv_idx(text) TYPE text(tokenizer = 'default')
    )
    ENGINE = MergeTree()
    ORDER BY key
    """)
    print("'littext' table created successfully.")

def insert_dataframe(client, table_name, df):
    """
    Inserts a pandas DataFrame into the specified ClickHouse table.
    """
    print(f"Inserting {len(df)} rows into '{table_name}'...")
    client.insert_df(table_name, df)
    print("Insertion complete.")

def write_df_to_sqlite(dataframe, table_name, db_name='openai_responses.db'):
    """
    Appends a DataFrame to a table in the SQLite database.
    """
    conn = sqlite3.connect(db_name)
    try:
        dataframe.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"Wrote {len(dataframe)} rows to SQLite table '{table_name}'.")
    finally:
        conn.close()

def update_sqlite_table(dataframe, key_col, table_name, db_name='openai_responses.db'):
    """
    Updates rows in an SQLite table based on a DataFrame.
    The DataFrame must contain the key column and the columns to be updated.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    try:
        db_cols = {col[1] for col in cursor.execute(f"PRAGMA table_info({table_name})").fetchall()}
        update_cols = list(set(dataframe.columns).intersection(db_cols) - {key_col})

        if not update_cols:
            print("No columns to update.")
            return

        set_clause = ", ".join([f"{col} = ?" for col in update_cols])
        update_query = f"UPDATE {table_name} SET {set_clause} WHERE {key_col} = ?"

        for _, row in dataframe.iterrows():
            values_to_update = tuple(row[col] for col in update_cols)
            key_value = row[key_col]
            cursor.execute(update_query, (*values_to_update, key_value))
        
        conn.commit()
        print(f"Updated {len(dataframe)} rows in SQLite table '{table_name}'.")
    finally:
        conn.close()
