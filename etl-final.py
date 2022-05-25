# modules 
import os
import mysql.connector
import pandas as pd
from google.cloud import bigquery

# variables inside IDE
current_path = os.getcwd()
load_file = 'mysql_export.csv'
load_file = os.path.join(current_path, 'data_files', load_file)
# inside bq 
proj = 'matthew-new-project'
data_set = 'data_set'
target_tabel = 'annual_movie_summary'
table_id = f'{proj}.{data_set}.{target_tabel}'

# connections 
conn = mysql.connector.connect(option_files='/Users/matthewking/Desktop/.my.cnf')
client = bigquery.Client(project=proj)

# create sql extract query
sql = """SELECT year, count(imdb_title_id) AS movie_count,
        ROUND(AVG(duration), 2) AS avg_movie_duration,
        ROUND(AVG(avg_vote), 2) AS avg_rating
        FROM oscarval_sql_course.imdb_movies
        GROUP BY 1
        """

# extract data into df 
df = pd.read_sql(sql, conn)

# transform the data with python function 
def year_rating(rating):
    if rating <= 5.65:
        return 'bad movie year'
    elif rating <=5.9:
        return 'okay movie year'
    elif rating <= 10:
        return 'good movie year'
    else:
        return 'unrated'

df['year_rating'] = df['avg_rating'].apply(year_rating)
df.to_csv(load_file, index=False)


# Load data 
job_config = bigquery.LoadJobConfig(
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
    write_disposition='WRITE_TRUNCATE'
)


# open file for loading 
with open(load_file, 'rb') as file:
    load_job = client.load_table_from_file(
        file,
        table_id,
        job_config=job_config
    )

# load job result
load_job.result()

# check how many records were loaded 
dest_table = client.get_table(table_id)
print(f'you have {dest_table.num_rows} rows in your table)')


