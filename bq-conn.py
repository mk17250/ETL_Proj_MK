# NOTE - different version of python must be set to run this, align python versions accross scripts, it is annoying. 
# modules 
from google.cloud import bigquery

# project location in bq
client = bigquery.Client(project='matthew-new-project')

# sql statement for data to query
sql = """SELECT * FROM data_set.movies_2005 LIMIT 5"""

# run query
query_job = client.query(sql)

# fetch results with results method 
results = query_job.result()

# print results 
for r in results:
    print(r.year, r.title, r.genre, r.avg_vote)
