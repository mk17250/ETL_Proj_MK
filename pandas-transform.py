import mysql.connector
import pandas as pd
import os 

# connectin from this file via virtual env to mysql db
conn = mysql.connector.connect(option_files='/Users/matthewking/Desktop/.my.cnf')

# path to current working directory, file name to create, 'data files' is the folder I created inside current folder for 
#  for data files to be stored, all joined to create current file path whihc is passed to create csv file later
cur_path = os.getcwd()
file = 'city_housing.csv'
file_path = os.path.join(cur_path, 'data_files', file)
print(file_path)

# sql query in python file
query = """SELECT * FROM oscarval_sql_course.city_house_prices"""

# sql query, and connection to esbalish query froom mysql db 
df = pd.read_sql(query, conn)

# data transformation steps 
df.set_index('Date', inplace=True)
df = df.stack().reset_index()
df.columns = ['Date', 'City', 'Price']

# create CSV from df
df.to_csv(file_path, index=False)
print(df)

conn.close()