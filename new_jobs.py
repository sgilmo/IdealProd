from sqlalchemy import create_engine
from sqlalchemy.types import Integer
from sqlalchemy.types import String
from sqlalchemy.types import DateTime

import pandas as pd
import urllib
import os


"""Get Current part data file from Filemaker database."""
server = 'tn-sql'
database = 'autodata'
driver = 'ODBC+Driver+13+for+SQL+Server'
user = 'production'
pwd = urllib.parse.quote_plus("Auto@matics")
port = '1433'
dsn = 'ACM'


database_conn = f'mssql+pyodbc://{user}:{pwd}@{server}:{port}/{database}?driver={driver}'

engine = create_engine(database_conn)
# conn = engine.connect()
print('Connected to SQL Server\n\n')

# Read data from CSV and load into a dataframe object
fpath = 'c:\\testdata\\'
labels = ['ID', 'JobComp', 'JobStart', 'Machine', 'PartNum', 'JobCnt', 'COTime', 'COType', 'SetupMan']
dtypes = {'ID' : String(),
          'JobComp': DateTime(),
          'JobStart': DateTime(),
          'Machine': String(),
          'PartNum': String(),
          'JobCnt': Integer(),
          'COTime': Integer(),
          'COType': String(),
          'SetupMan': String()
         }
filelist = os.listdir(fpath)
for filename in filelist:
    jdata = pd.read_csv(fpath + filename, names=labels, index_col=False)
    jdata.to_sql('MachJobs2', index=False, con=engine, schema='production', if_exists='append', dtype=dtypes)