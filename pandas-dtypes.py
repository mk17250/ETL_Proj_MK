import mysql.connector
import pandas as pd
conn = mysql.connector.connect(option_files='/Users/matthewking/Desktop/.my.cnf')

query = "SELECT year, title, genre, avg_vote \
        FROM oscarval_sql_course.imdb_movies \
        LIMIT 7"
    
df = pd.read_sql(query, conn)
print(df)

print(df.dtypes)

conn.close()