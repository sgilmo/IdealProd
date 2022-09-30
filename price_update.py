"""Update Automation Direct Price Sheet.

Compare existing file on server with Automation Directs
If there is a difference replace the one on the server.
"""

import pyodbc
import pandas as pd
from timeit import default_timer as timer

# Define Database Connection

CONNECTION = """
Driver={SQL Server};
Server=tn-sql;
Database=autodata;
UID=production;
PWD=Auto@matics;
"""


def read_pricelist():
    """Open price list workbook"""
    dbase = []
    print('Fetching Data File From Automation Direct Web Site')
    url = 'https://cdn.automationdirect.com/static/prices/prices_public.xlsx'
    # url = 'c:\\PriceLists\\prices_public.xlsx'
    df = pd.read_excel(url, sheet_name='ADC Price List with Categories ',
                       usecols=[0, 2, 3])
    validity = df.values[3, 0]
    for item in df.values:
        cost = item[2]
        if type(cost) == float or type(cost) == int:
            cost = float(cost)
            if cost > 0.00:
                dbase.append(list([item[0], item[2], item[1] + ' ' + validity]))
    return dbase


def update_db(dbase):
    """ Add prices to SQL server database"""
    dbcnxn = pyodbc.connect(CONNECTION)
    cursor = dbcnxn.cursor()
    cursor.fast_executemany = True
    # Delete Existing Records
    print("Deleting Existing Records on SQL Server")
    cursor.execute("TRUNCATE TABLE production.Adirect")
    dbcnxn.commit()

    # Load price data onto SQL server
    print("Loading data to SQL server")
    strsql = "INSERT INTO production.Adirect (part,price,status) VALUES (?,?,?)"
    cursor.executemany(strsql, dbase)
    dbcnxn.commit()
    print(str(len(dbase)) + " Records Processed")
    return


def main():
    """Main Function"""
    start = timer()
    update_db(read_pricelist())
    print("Total Time = " + str(round((timer() - start), 3)) + " sec")


if __name__ == '__main__':
    main()
