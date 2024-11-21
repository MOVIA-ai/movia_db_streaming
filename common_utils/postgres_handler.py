import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


class PostgresHandler:
    def __init__(self):
        load_dotenv()
        
        # Initialize connection engine
        self.engine = self.create_engine()

    def create_engine(self):
        """
        Creates an SQLAlchemy engine using environment variables for connection details.
        """
        try:
            db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
                     f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            engine = create_engine(db_url)
            return engine
        except SQLAlchemyError as e:
            print(f"Failed to create engine: {e}")
            raise

    def query_to_df(self, query):
        """
        Executes a SQL query and returns the result as a pandas DataFrame.
        """
        try:
            with self.engine.connect() as connection:
                df = pd.read_sql_query(query, connection)
            return df
        except SQLAlchemyError as e:
            print(f"Query failed: {e}")
            raise

    def read_table(self, table_name):
        """
        Reads an entire table into a pandas DataFrame.
        """
        query = f"SELECT * FROM {table_name}"
        try:
            df = self.query_to_df(query)
            return df
        except SQLAlchemyError as e:
            print(f"Failed to read the table: {e}")
            raise

    def close(self):
        """
        Disposes of the SQLAlchemy engine, closing all connections.
        """
        self.engine.dispose()


# import psycopg2
# import pandas as pd
# from dotenv import load_dotenv
# import os


# class PostgresHandler:
#     def __init__(self):
#         load_dotenv()
        
#         self.conn = None
#         self.connect()  # Open connection during initialization

#     def connect(self):
#         """
#         Establishes a new connection to the PostgreSQL database using environment variables.
#         """
#         if self.conn is None or self.conn.closed:
#             try:
#                 self.conn = psycopg2.connect(
#                     host=os.getenv("DB_HOST"),
#                     database=os.getenv("DB_NAME"),
#                     user=os.getenv("DB_USER"),
#                     password=os.getenv("DB_PASSWORD"),
#                     port=os.getenv("DB_PORT")
#                 )
#             except Exception as e:
#                 print(f"Failed to connect to the database: {e}")
#                 raise

#     def query_to_df(self, query):
#         """
#         Executes a SQL query and returns the result as a pandas DataFrame.
#         """
#         try:
#             self.connect()  # Ensure connection is open before querying
#             df = pd.read_sql_query(query, self.conn)
#             return df
#         except Exception as e:
#             print(f"Query failed: {e}")
#             raise
#         finally:
#             self.conn.commit()  # Ensure any changes are committed

#     def close(self):
#         """
#         Closes the database connection.
#         """
#         if self.conn:
#             self.conn.close()

#     def read_table(self, table_name):
#         query = f"SELECT * FROM {table_name}"
        
#         try:
#             df = self.query_to_df(query)
#             return df
#         except Exception as e:
#             print(f"Failed to read the table: {e}")
#             raise