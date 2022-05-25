# PREFERRED FINAL METHOD!!!!!
# NOTE - this does not creare a CVSin local drive, this extracts data from MySQL, transforms it in the script via df, 
# then loads to bigquery
# modules 
import mysql.connector
import pandas as pd
from google.cloud import bigquery


# variables 
proj = 'matthew-new-project'
data_set = 'data_set'
target_tabel = 'annual_movie_summary_df'
table_id = f'{proj}.{data_set}.{target_tabel}'

# connections to bigquery
conn = mysql.connector.connect(option_files='/Users/matthewking/Desktop/.my.cnf')
client = bigquery.Client(project=proj)

# create sql extract query
sql = """SELECT year, count(imdb_title_id) AS movie_count,
        ROUND(AVG(duration), 2) AS avg_movie_duration,
        ROUND(AVG(avg_vote), 2) AS avg_rating
        FROM oscarval_sql_course.imdb_movies
        GROUP BY 1
        """

# extract data, here we read data from mysql db 
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

# here we use the function created and apply it to our df and transform data
df['year_rating'] = df['avg_rating'].apply(year_rating)

# set job config
job_config = bigquery.LoadJobConfig(
    autodetect=True,
    write_disposition='WRITE_TRUNCATE'
)

# NOTE - THIS HAS BEEN AMENDED AS DO NOT NEED TO OPEN FILE LOCALLY, WE ARE LOADING FROM DF 
# load data from df to gogle cloud bigquery
load_job = client.load_table_from_dataframe(
    df,
    table_id,
    job_config=job_config
)

# load job result - check the docs for this step.
load_job.result()

# check how many records were loaded 
dest_table = client.get_table(table_id)
print(f'you have {dest_table.num_rows} rows in your table)')


