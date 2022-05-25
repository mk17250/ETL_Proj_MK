
import mysql.connector
import pandas as pd
import os 

# connectin from this file via virtual env to mysql db
conn = mysql.connector.connect(option_files='/Users/matthewking/Desktop/.my.cnf')

# path to current working directory, file name to create, 'data files' is the folder I created inside current folder for 
#  for data files to be stored, all joined to create current file path whihc is passed to create csv file later
cur_path = os.getcwd()
file = 'movie_rating.csv'
file_path = os.path.join(cur_path, 'data_files', file)
print(file_path)

# sql query in python file
query = """ SELECT year, title, genre, avg_vote, 
            CASE 
                WHEN avg_vote < 3 THEN 'BAD' 
                WHEN avg_vote < 6 THEN 'OKAY' 
                WHEN avg_vote >= 6 THEN 'GOOD' 
            END AS 'Movie Rating' 
            FROM oscarval_sql_course.imdb_movies 
            WHERE year BETWEEN 2005 AND 2010 """

# sql query, and connection to esbalish query froom mysql db 
df = pd.read_sql(query, conn)

# create CSV from df
# df[year_2005].to_csv(file_path, index=False)
df.to_csv(file_path, index=False)

conn.close()