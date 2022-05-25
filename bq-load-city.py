# NOTE - Different version of python required than set in pyenv virtual env
# To set up virtual env, pyenv activate etl1_proj1, in terminal  
# modules 
from gzip import WRITE
from google.cloud import bigquery
import os

from numpy import WRAP

# set path to project in bq, and path to table with project, db, then create table name being created 
client = bigquery.Client(project='matthew-new-project')
target_table = 'matthew-new-project.data_set.city_housing'

# set job configuration 
# NOTE - if writeDisposition parameter in the below config left to feault will APPEND data (WRITE_APPEND),
#  in order to overwrite set to WRITE_TRUNCATE
job_config = bigquery.LoadJobConfig(
    skip_leading_rows = 1,
    source_format = bigquery.SourceFormat.CSV,
    autodetect = True,
    write_disposition = 'WRITE_TRUNCATE'
)
#  file vars, from local env to push to bq 
cur_path = os.getcwd()
file = 'city_housing.csv'
file_path = os.path.join(cur_path, 'data_files', file)

# using rb (read binar) open file from local and set config 
with open(file_path, 'rb') as source_file:
    load_job = client.load_table_from_file(
        source_file,
        target_table,
        job_config=job_config
    )
#  fetch results
load_job.result()

# set table attributes 
destination_table = client.get_table(target_table)
print(f"you have {destination_table.num_rows} rows in your table")

# NOTE - THIS METHOD APPENDS DATA TO EXISTING TABLE, SO IF YOU KEEP RUNNING SCRIPT YOU WILL KEEP APPENDING DATA TO TABLE. 