
from sqlalchemy import create_engine
import pandas as pd
import urllib

"""Get Current part data file from Filemaker database."""
server = 'tn-sql'
database = 'autodata'
driver = 'ODBC+Driver+13+for+SQL+Server'
user = 'production'
pwd = urllib.parse.quote_plus("Auto@matics")
port = '1433'
dsn = 'ACM'
database_conn = f'mssql+pyodbc://{user}:{pwd}@{server}:{port}/{database}?driver={driver}'
connection_string = 'mssql+pyodbc://demo:%s@localhost/AdventureWorks2014?driver=SQL Server' % urllib.parse.quote_plus("Auto@matics")

print(database_conn)
engine = create_engine(database_conn)
conn = engine.connect()


print('Connected to SQL Server\n\n')

sql = "SELECT * FROM autodata.production.parts"
print(sql)
df = pd.read_sql(sql, conn)

print(df)

if len(df) == 0:
    print("No Part Found")
