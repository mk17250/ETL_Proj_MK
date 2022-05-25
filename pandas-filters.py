import mysql.connector
import pandas as pd
conn = mysql.connector.connect(option_files='/Users/matthewking/Desktop/.my.cnf')

query = "SELECT year, title, genre, avg_vote \
        FROM oscarval_sql_course.imdb_movies \
        WHERE year BETWEEN 2005 AND 2006"
    
df = pd.read_sql(query, conn)

year_2005 = df['year'] == 2005

# HANDY TIP! In this instance the ~ (tilda) sign is the same as != therefore, all 2006 movies being returned 
print(df[~year_2005].head(10))

conn.close()