import mysql.connector
conn = mysql.connector.connect(option_files='/Users/matthewking/Desktop/.my.cnf')

cursor = conn.cursor()

query = "SELECT year, title, genre \
        FROM oscarval_sql_course.imdb_movies \
        LIMIT 7"

cursor.execute(query)
result = cursor.fetchall()
print("Total number of rows in table: ", cursor.rowcount)

# for (year, title, genre) in result:
#     print(year, title, genre)


print("Printing each row \n")
for row in result:
    print("Year  = ", row[0], )
    print("Title = ", row[1])
    print("Genre = ", row[2], '\n')
    
    

conn.close()