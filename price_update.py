"""Update Automation Direct Price Sheet.

Compare existing file on server with Automation Directs
If there is a difference replace the one on the server.
"""

import xlrd
import pyodbc
import urllib.request
import urllib.parse
import urllib.error
import platform
import contextlib
from timeit import default_timer as timer

# Define Database Connection

CONNECTION = """
Driver={SQL Server Native Client 11.0};
Server=tn-sql14;
Database=autodata;
UID=production;
PWD=Auto@matics;
"""
if platform.release() == 'XP':
    CONNECTION = """
    Driver={SQL Server Native Client 10.0};
    Server=tn-sql14;
    Database=autodata;
    UID=production;
    PWD=Auto@matics;
    """


def get_pricelist():
    """Get Price List From Automation Direct Web Site"""
    url = 'https://cdn.automationdirect.com/static/prices_public.xls'
    filename = "c:\\PriceLists\\prices_public.xlsx"
    print("downloading Price List")
    with open(filename, 'wb') as out_file:
        with contextlib.closing(urllib.request.urlopen(url)) as fp:
            block_size = 1024 * 8
            while True:
                block = fp.read(block_size)
                if not block:
                    break
                out_file.write(block)
    return


def read_pricelist():
    """Open price list workbook"""
    workbook = xlrd.open_workbook('c:\\PriceLists\\prices_public.xlsx')
    print("Opening Workbook for parsing")
    worksheet = workbook.sheet_by_name('priceslive')
    num_rows = worksheet.nrows - 1
    curr_row = 2
    part_col = 0
    price_col = 3
    dbase = []
    while curr_row < num_rows:
        curr_row += 1
        part = worksheet.cell_value(curr_row, part_col)
        price = worksheet.cell_value(curr_row, price_col)
        if type(price) is float:
            dbase.append(list([str(part), float(price)]))
    print(repr(num_rows) + " Rows Found on Spreadsheet")
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
    strsql = "INSERT INTO production.Adirect (part,price) VALUES (?,?)"
    cursor.executemany(strsql, dbase)
    dbcnxn.commit()
    print(str(len(dbase)) + " Records Processed")
    return


def main():
    """Main Function"""
    start = timer()
    get_pricelist()
    update_db(read_pricelist())
    print("Total Time = " + str(round((timer() - start), 3)) + " sec")


if __name__ == '__main__':
    main()
