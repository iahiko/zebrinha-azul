import os
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from dotenv import find_dotenv, load_dotenv
class DatabaseOps:
    def __init__(self, database='zebrinha_azul'):

        load_dotenv(find_dotenv())
        """
        Initialize the DatabaseOps class.

        Args:
            server (str): The server name.
            database (str): The database name.
            username (str): The username for authentication.
            password (str): The password for authentication.
        """
        self.server = os.getenv('SERVER')
        self.database = database

    def connect_db(self):
        """
        Connect to the SQL Server database.

        Returns:
            pyodbc.Connection: The database connection object.
            pyodbc.Database: The database object.
        """
        try:
            # Define the connection string
            conn_str = f"\
                DRIVER={{ODBC Driver 17 for SQL Server}};\
                SERVER={self.server};\
                DATABASE={self.database};\
                Trusted_Connection=yes"

            # Establish the database connection
            self.conn = pyodbc.connect(conn_str)
            self.db = self.conn.cursor().connection
            self.engine =  create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")
            print(f"Connected to the database {self.database}.")

            return None

        except pyodbc.Error as e:
            print("An error occurred while connecting to the database:", e)


    def run_query(self, query):
        try:
            # Execute the query and return the result as a DataFrame
            df = pd.read_sql(query, self.engine)
            return df
        except Exception as e:
            print(f"An error occurred while executing the query: {e}")
            return None

    


    def insert(self, dataframe, schema, table, if_exists='replace'):
        """
        Insert data into the specified table in the database.

        Args:
            table (str): The name of the table.
            dataframe (pd.DataFrame): The DataFrame containing the data to be inserted.
            schema (str): The name of the schema.
            if_exists (str, optional): The action to take if the table already exists.
                Defaults to 'replace'.
        """
        try:
            # Check if the schema exists
            schema_query = f"SELECT COUNT(*) as count FROM information_schema.schemata WHERE schema_name = '{schema}'"
            schema_exists = self.run_query(schema_query)['count'][0] > 0
            # print('Schema exists? ', schema_exists)
            
            if not schema_exists:
                # Create the schema if it doesn't exist
                create_schema_query = f"CREATE SCHEMA {schema}"
                self.conn.execute(create_schema_query)
                # print(f"Schema '{schema}' created successfully!")

            # Insert the DataFrame into the SQL Server table with the if_exists option
            # print('Insert data to DB')
            dataframe.to_sql(schema=schema, name=table, con=self.engine, if_exists=if_exists, index=False)
            # print("Data inserted successfully!")

        except pyodbc.Error as e:
            print("An error occurred while inserting the data:", e)