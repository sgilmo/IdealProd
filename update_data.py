# coding=utf-8
#!/usr/bin/env python

"""Pandas Driven Data Updates to SQL Server. Will Eventually replace updates and getclamps"""
import csv

import pandas as pd
import pyodbc
import os
import sqlalchemy.types
from sqlalchemy import create_engine
from urllib import parse
from timeit import default_timer as timer


# Define Database Connections

CONNAS400_PROD = """
Driver={iSeries Access ODBC Driver};
system=10.143.12.10;
Server=AS400;
Database=PROD;
UID=SMY;
PWD=SMY;
"""

CONNAS400_CCSDTA = """
Driver={iSeries Access ODBC Driver};
system=10.143.12.10;
Server=AS400;
Database=CCSDTA;
UID=SMY;
PWD=SMY;
"""

CONNFM = 'DSN=FM Clamp ODBC;UID=FMODBC;PWD=FMODBC'

server = 'tn-sql'
database = 'autodata'
driver = 'ODBC+Driver+17+for+SQL+Server'
user = 'production'
pwd = parse.quote_plus("Auto@matics")
port = '1433'
database_conn = f'mssql+pyodbc://{user}:{pwd}@{server}:{port}/{database}?driver={driver}'
# Make Connection
engine = create_engine(database_conn)
conn_sql = engine.connect()

# Define Some SQL Statements

sql_inv = """
SELECT
    STRIP(y.itmid),
    b.qty,
    STRIP(y.itmdesc),
    STRIP(SUBSTR (Altdesc, 15, 1)) Class
FROM
    CCSDTA.DCSCIM y,
    CCSDTA.DMFCMAR x,
    CCSDTA.DCSILM b
WHERE
    x.itmid = y.itmid
    AND x.itmid = b.itmid
    AND x.plt = b.plt
    AND b.plt = '09'
    AND qty <> 0
    AND x.COSTID = 'FRZ'
    AND x.plt NOT IN ('53', '54', '55', '56', '59')
"""

sql_parts = """
    SELECT Ourpart,"Band A Part Number", "Housing A Part Number",
        "Screw Part Number", "Band Feed from Band data",
        "Ship Diam Max", "Ship Diam Min", "Hex Size", "Band_Thickness", "Band_Width",
        "CameraInspectionRequired", "ScrDrvChk", "Cutout1", "DrawingPrintNumber","Size", "Pack"
    FROM tbl8Tridon
"""

# File path constants
CSV_OUTPUT_PATH = 'c:\\temp\\'  # Change to your desired output directory


def pull_data(conn,qry):
    # Connection with error handling and connection management
    result = []
    msg = ""  # Initialize msg
    dbcnxn = None  # Initialize dbcnxn
    try:
        dbcnxn = pyodbc.connect(conn, timeout=30)
        cursor = dbcnxn.cursor()
        start = timer()

        try:
            cursor.execute(qry)
            result = cursor.fetchall()
            msg = str(len(result)) + " Records Processed From Table"
        except Exception as e:
            msg = conn + ' Query Failed: ' + str(e)
            print(msg)
    except pyodbc.Error as e:
        msg = conn + ' Connection Failed: ' + str(e)
        print(msg)
    finally:
        if msg:
            print(msg)
        if dbcnxn is not None:
            try:
                print("Connect/Query Time = " + str(round((timer() - start), 3)) + " sec")
                dbcnxn.close()
            except:
                pass  # Handle case where connection wasn't established

    return result

def comp_df():
    # Build Components Dataframe
    print('Getting Data From AS400')
    data = pull_data(CONNAS400_CCSDTA, sql_inv)

    if not data:
        print("Warning: No data retrieved from AS400")
        return pd.DataFrame()

    df_inv = pd.DataFrame.from_records(data)
    data_type_dict = {'ITMID': str, 'QTY': int, 'ITMDESC': str, 'CLASS': str}
    df_inv.columns = ['ITMID', 'QTY', 'ITMDESC', 'CLASS']

    # Clean data before type conversion
    df_inv = df_inv.dropna()

    # Convert QTY to numeric, coercing errors
    df_inv['QTY'] = pd.to_numeric(df_inv['QTY'], errors='coerce')
    df_inv = df_inv.dropna(subset=['QTY'])  # Remove rows where QTY couldn't be converted

    try:
        df_inv = df_inv.astype(data_type_dict)
    except Exception as e:
        print(f"Error converting data types: {e}")
        return pd.DataFrame()

    df_inv = df_inv.convert_dtypes()
    print(f"Processed {len(df_inv)} records")
    return df_inv

def parts_df():
    # Build Parts Dataframe
    print('Getting Data From Filemaker')
    data = pull_data(CONNFM,sql_parts)
    if not data:
        print("Warning: No data retrieved from Filemaker")
        return pd.DataFrame()
    df_clamps = pd.DataFrame.from_records(data)
    # Set Column Names
    df_clamps.columns = ['PartNumber', 'Band', 'Housing', 'Screw', 'Feed', 'DiaMax', 'DiaMin', 'HexSz', 'BandThickness',
                         'BandWidth', 'CamInspect', 'ScrDrvChk', 'Cutout1', 'Drawing', 'Size', 'Pack']
    # Set Data Types
    data_type_dict = {'PartNumber': str, 'Band': str, 'Housing': str, 'Screw': str, 'Feed': float, 'DiaMax': float,
                      'DiaMin': float, 'HexSz': str, 'BandThickness': float, 'BandWidth': float, 'CamInspect': str,
                      'ScrDrvChk': str, 'Cutout1': float, 'Drawing': str, 'Size': str, 'Pack': str}

    # Do Some Filtering and Data Cleansing
    df_clamps = df_clamps[df_clamps.Feed != 'N/A']
    df_clamps = df_clamps[1:]

    # Trim whitespace from all string columns
    string_cols = df_clamps.select_dtypes(include=['object']).columns
    df_clamps[string_cols] = df_clamps[string_cols].apply(lambda x: x.str.strip())

    # Remove Quotes
    df_clamps[string_cols] = df_clamps[string_cols].apply(lambda x: x.str.replace(r'["\']', '', regex=True))

    try:
        df_clamps = df_clamps.astype(data_type_dict)
    except Exception as e:
        print(f"Error converting data types: {e}")
        return pd.DataFrame()
    # df_clamps = df_clamps[df_clamps.Size != 'None']
    df_clamps = df_clamps[df_clamps.Band != 'None']
    df_clamps = df_clamps[df_clamps.Housing != 'None']
    df_clamps = df_clamps[df_clamps.Screw != 'None']
    df_clamps = df_clamps[df_clamps.HexSz != 'None']
    df_clamps = df_clamps[df_clamps.CamInspect != 'None']
    df_clamps = df_clamps[df_clamps.PartNumber != 'None']
    df_clamps['CamInspect'] = df_clamps['CamInspect'].str.upper()
    df_clamps['ScrDrvChk'] = df_clamps['ScrDrvChk'].str.upper()
    df_clamps['Feed'] = df_clamps['Feed'].round(3)
    df_clamps['BandWidth'] = df_clamps['BandWidth'].round(3)
    df_clamps['BandThickness'] = df_clamps['BandThickness'].round(3)
    df_clamps.fillna({'DiaMax': 0.0}, inplace=True)
    df_clamps.fillna({'DiaMin': 0.0}, inplace=True)
    df_clamps.fillna({'Cutout1': 0.0}, inplace=True)
    df_clamps['DiaMax'] = df_clamps['DiaMax'].round(3)
    df_clamps['DiaMin'] = df_clamps['DiaMin'].round(3)
    df_clamps['Cutout1'] = df_clamps['Cutout1'].round(3)
    df_clamps['HexSz'] = df_clamps['HexSz'].replace('Purchased 5/16', '5/16')
    df_clamps['HexSz'] = df_clamps['HexSz'].replace('MX ONLY 10 mm ss', '10 mm')
    df_clamps['HexSz'] = df_clamps['HexSz'].replace('7 mm Umbrella', '7 mm')
    df_clamps['HexSz'] = df_clamps['HexSz'].replace('10 mm SS MX ONLY', '10 mm')
    df_clamps = df_clamps.dropna()
    df_clamps = df_clamps.convert_dtypes()
    df_clamps.drop_duplicates(subset='PartNumber', keep='first', inplace=True)
    df_clamps = df_clamps.sort_values(by='PartNumber')
    print(f"Processed {len(df_clamps)} records")
    return df_clamps

def part_tbl(df_data):
    # Build Parts Table
    print('Build Part SQL Table')
    if df_data.empty:
        print("Warning: Empty DataFrame, skipping SQL insert")
        return
    data_type_dict = {'PartNumber': sqlalchemy.types.VARCHAR(255), 'Band': sqlalchemy.types.VARCHAR(255),
                      'Housing': sqlalchemy.types.VARCHAR(255), 'Screw': sqlalchemy.types.VARCHAR(255),
                      'Feed': sqlalchemy.types.Float, 'DiaMax': sqlalchemy.types.Float,
                      'DiaMin': sqlalchemy.types.Float, 'HexSz': sqlalchemy.types.VARCHAR(255),
                      'BandThickness': sqlalchemy.types.Float, 'BandWidth': sqlalchemy.types.Float,
                      'CamInspect': sqlalchemy.types.VARCHAR(255), 'ScrDrvChk': sqlalchemy.types.VARCHAR(255),
                      'Cutout1': sqlalchemy.types.Float, 'Drawing': sqlalchemy.types.VARCHAR(255),
                      'Size': sqlalchemy.types.VARCHAR(255), 'Pack': sqlalchemy.types.VARCHAR(255)}
    try:
        df_data.to_sql('parts_clamps', conn_sql, schema='production', if_exists='replace', index=False,
                         dtype=data_type_dict)
        print(f"Successfully inserted {len(df_data)} records into parts_clamps")
    except Exception as e:
        print(f"Error inserting data into parts_clamps: {e}")

def comp_tbl(df_data):
    # Build Components Table
    print('Build Component SQL Table')
    if df_data.empty:
        print("Warning: Empty DataFrame, skipping SQL insert")
        return
    data_type_dict = {'ITMID': sqlalchemy.types.VARCHAR(255), 'QTY': sqlalchemy.types.INT,
                      'ITMDESC': sqlalchemy.types.VARCHAR(255),
                      'CLASS': sqlalchemy.types.VARCHAR(255)}
    try:
        df_data.to_sql('tblCompInv', conn_sql, schema='production', if_exists='replace', index=False,
                         dtype=data_type_dict)
        print(f"Successfully inserted {len(df_data)} records into tblCompInv")
    except:
        print("Error inserting data into tblCompInv")


def save_dataframe_to_csv(df_data, filename, output_path=CSV_OUTPUT_PATH):
    """
    Save DataFrame to CSV file.

    Args:
        df_data: DataFrame to save
        filename: Name of the CSV file (without path)
        output_path: Directory path where file will be saved

    Returns:
        str: Full path of saved file or None on error
    """
    if df_data.empty:
        print(f"Warning: Empty DataFrame, skipping CSV export for {filename}")
        return None

    try:
        # Ensure filename has .csv extension
        if not filename.endswith('.csv'):
            filename += '.csv'

        full_path = os.path.join(output_path, filename)

        # Create directory if it doesn't exist
        os.makedirs(output_path, exist_ok=True)

        # Write to CSV
        df_data.to_csv(full_path, index=False, sep=',', encoding='utf-8', quoting=csv.QUOTE_MINIMAL, header=False)

        print(f"Successfully saved {len(df_data)} records to {full_path}")
        return full_path

    except Exception as e:
        print(f"Error saving DataFrame to CSV {filename}: {e}")
        return None


def main():
    try:
        # Get data
        df_parts = parts_df()
        df_components = comp_df()

        # Save to SQL Server
        part_tbl(df_parts)
        comp_tbl(df_components)

        # Save to CSV files
        save_dataframe_to_csv(df_parts, 'parts_clamps.csv')
        save_dataframe_to_csv(df_components, 'components_inventory.csv')

    except Exception as e:
        print(f"Error in main: {e}")
    finally:
        # Close the connection
        if 'conn_sql' in globals():
            conn_sql.close()
            engine.dispose()
    return

if __name__ == '__main__':
    main()