"""Update Automation Direct Price Sheet.

Pull Current Pricelist from Automation Direct Website and
Load it on to existing SQL Server Table
"""
import pandas as pd
from sqlalchemy import create_engine
from urllib import parse
from timeit import default_timer as timer
import requests
import io


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
conn = engine.connect()


def read_pricelist():
    """Open price list workbook"""
    import requests
    import io
    import pandas as pd
    
    print('Fetching Data File From Automation Direct Web Site')
    
    name = 'ADC Price List with Categories '
    url = 'https://cdn.automationdirect.com/static/prices/prices_public.xlsx'
    
    # Create browser-like headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/vnd.ms-excel',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.automationdirect.com/'
    }
    
    # Make the request with appropriate headers
    response = requests.get(url, headers=headers, verify=True)
    
    # Check if request was successful
    if response.status_code != 200:
        raise Exception(f"Failed to download file: HTTP {response.status_code}")
    
    # Read the Excel data
    excel_data = io.BytesIO(response.content)
    df = pd.read_excel(excel_data, sheet_name=name, usecols=[0, 2, 3])
    
    df_dropped = df.dropna()
    df_reset = df_dropped.reset_index(drop=True)
    df_reset.columns = ['part', 'status', 'price']
    df_final = df_reset[1:]
    return df_final


def update_db(df):
    """ Add prices to SQL server database"""
    print(df)
    df.to_sql('Adirect', engine, schema='production', if_exists='replace', index=False)
    return


def main():
    """Main Function"""
    start = timer()
    update_db(read_pricelist())
    print("Total Time = " + str(round((timer() - start), 3)) + " sec")


if __name__ == '__main__':
    main()
