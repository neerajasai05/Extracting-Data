import requests
import pandas as pd
import xlrd
import openpyxl
import psycopg2
import numpy as np
from openpyxl import load_workbook
from openpyxl.styles import numbers
from sqlalchemy import create_engine
from psycopg2 import OperationalError



 
#create a data base connection and use it for all calls and at the end of the process close the ConnectionAbortedError
#this should start with try and catch following with finally bloack( close the data base connection in finally block)


url = "https://pages.stern.nyu.edu/~adamodar/pc/datasets/insholdEurope.xls"
response =requests.get(url, verify=False) #(verify= False)Avoid ssl certification
if response.status_code ==200:
        file_path = 'file.xlsx'
        with open('file.xlsx', 'wb') as file:
                file.write(response.content)
                print("File downloaded successfully")
    
        try:
                # Read the .xlsx file into a pandas DataFrame
                df = pd.read_excel(file_path, engine='xlrd', sheet_name="Industry Averages",skiprows=7)
                
        
                # Save the DataFrame to an Excel file
                df.to_excel('output.xlsx', index=False)

                #print("Data in output.xlsx:")
                #print(df)

                print("Data has been successfully saved to output.xlsx")
                # Remove duplicate rows
                df.drop_duplicates(inplace=True)

                # Handle missing data
                df.fillna(' ', inplace=True)
                df.dropna(inplace=True)
                # Print the column names to verify their exact names
                #print("Column Names:")
                #print(df.columns)

                # Strip any leading or trailing spaces from column names
                df.columns = df.columns.str.strip()
                percentage_columns = ['CEO Holding', 'Corporate Holdings', 'Institutional Holdings', 'Insider Holdings']
                
                # Convert the specified columns to percentage
                float_columns = df.select_dtypes(include=['float64'])
                for column in percentage_columns:
                        df[column] = round(df[column] * 100, 2).astype(str) + '%'
                #print(df)
                conn = psycopg2.connect( user = "postgres", password = "root", host = "localhost" , port = 5432, dbname="Corporate_Governments",)
                status = conn.status
                # Print the connection status
                print(status)
                print("Database connected successfully")
                
                cursor = conn.cursor()
                print("cursor completed")
                
                connection_string = ("postgresql+psycopg2://postgres:root@localhost:5432/Corporate_Governments")
                engine = create_engine(connection_string)
                print("engine created")
                table_name = 'us'  # Replace with your table name
                df.to_sql(table_name, engine, if_exists='replace', index=False)
                #print("df to table")
                sql_query = "SELECT * FROM us;"
                cursor.execute(sql_query)
                print("Query Execured")
    
                # Fetch result
                #result = cursor.fetchall()
                #for row in result:
                #print(row)
                

                # Step 3: Insert DataFrame into the PostgreSQL database
                
                print("Data has been successfully inserted into the database.")
                
                

        except Exception as e:
                print(f"An error occurred: {e}")
                
        finally :
               conn_close = conn.close()
               print(conn_close)
               print("DB connection closed")
                                     
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")


    