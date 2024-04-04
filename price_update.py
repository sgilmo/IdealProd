"""Update Automation Direct Price Sheet.

Compare existing file on server with Automation Directs
If there is a difference replace the one on the server.
"""

import pandas as pd
from sqlalchemy import create_engine
from urllib import parse
from timeit import default_timer as timer

# Define Database Connection

# SQLAlchemy connection
server = 'tn-sql'
database = 'autodata'
driver = 'ODBC+Driver+17+for+SQL+Server'
user = 'production'
pwd = parse.quote_plus("Auto@matics")
port = '1433'
database_conn = f'mssql+pyodbc://{user}:{pwd}@{server}:{port}/{database}?driver={driver}'
# Make Connection
engine = create_engine(database_conn)
# conn = engine.raw_connection()
conn = engine.connect()


def read_pricelist():
    """Open price list workbook"""
    name = 'ADC Price List with Categories '
    url = 'https://cdn.automationdirect.com/static/prices/prices_public.xlsx'
    print('Fetching Data File From Automation Direct Web Site')
    df = pd.read_excel(url, sheet_name=name, usecols=[0, 2, 3])
    df_dropped = df.dropna()
    df_reset = df_dropped.reset_index(drop=True)
    df_reset.columns = ['part', 'status', 'price']
    df_final = df_reset[1:]
    return df_final


def update_db(df):
    """ Add prices to SQL server database"""
    df.to_sql('Adirect', conn, schema='production', if_exists='replace', index=False)
    return


def main():
    """Main Function"""
    start = timer()
    update_db(read_pricelist())
    print("Total Time = " + str(round((timer() - start), 3)) + " sec")


if __name__ == '__main__':
    main()
