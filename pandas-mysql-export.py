# exporting to CSv

import mysql.connector
import pandas as pd
import os 

# connectin from this file via virtual env to mysql db
conn = mysql.connector.connect(option_files='/Users/matthewking/Desktop/.my.cnf')

# path to current working directory, file name to create, 'data files' is the folder I created inside current folder for 
#  for data files to be stored, all joined to create current file path whihc is passed to create csv file later
cur_path = os.getcwd()
file = 'movies_2005.csv'
file_path = os.path.join(cur_path, 'data_files', file)
print(file_path)

# sql query in python file
query = "SELECT year, title, genre, avg_vote \
        FROM oscarval_sql_course.imdb_movies \
        WHERE year BETWEEN 2005 AND 2010"

# sql query, and connection to esbalish query froom mysql db 
df = pd.read_sql(query, conn)

# filter, if required, demonstrating how pandas can be used to filter data once extracted 
year_2005 = df['year'] == 2005

# HANDY TIP! In this instance the ~ (tilda) sign is the same as != therefore, all 2006 movies being returned 
print(df[~year_2005].head(10))

# create CSV from df
# df[year_2005].to_csv(file_path, index=False)
df[year_2005].to_csv(file_path, index=False)

conn.close()