import mysql.connector
import pandas as pd
import os 

# connectin from this file via virtual env to mysql db
conn = mysql.connector.connect(option_files='/Users/matthewking/Desktop/.my.cnf')

# path to current working directory, file name to create, 'data files' is the folder I created inside current folder for 
#  for data files to be stored, all joined to create current file path whihc is passed to create csv file later
cur_path = os.getcwd()
file = 'movies_length.csv'
file_path = os.path.join(cur_path, 'data_files', file)
print(file_path)

# sql query in python file
query = """ SELECT year, title, genre, avg_vote, 
            CASE 
                WHEN avg_vote < 3 THEN 'BAD' 
                WHEN avg_vote < 6 THEN 'OKAY' 
                WHEN avg_vote >= 6 THEN 'GOOD' 
            END AS 'Movie Rating', duration
            FROM oscarval_sql_course.imdb_movies 
            WHERE year BETWEEN 2005 AND 2010 """

#  duration function label 
def movie_duration(duration):
    if duration < 60:
        return 'short movie'
    elif duration < 90:
        return 'medium length movie'
    elif duration < 100000000:
        return 'long movie'
    else:
        return 'no data'

# sql query, and connection to esbalish query froom mysql db 
df = pd.read_sql(query, conn)

df['Duration'] = df['duration'].apply(movie_duration)

# create CSV from df
# df[year_2005].to_csv(file_path, index=False)
df.to_csv(file_path, index=False)

conn.close()

# end