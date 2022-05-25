
import mysql.connector
from mysql.connector import errorcode

try:
    conn = mysql.connector.connect(option_files='/Users/matthewking/Desktop/.my.cnf')
    print('connection sucessful')
    conn.close()
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('check credentials')
    else:
        print('err')

