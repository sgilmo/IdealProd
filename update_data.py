# coding=utf-8
#!/usr/bin/env python

"""Pandas Driven Data Updates to SQL Server. Will Eventually replace updates and getclamps"""
import csv
import pandas as pd
import pyodbc
import os
import socket
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

sql_bands = """
    SELECT "Band Part Number","Material Spec Number", "Number of Notches",
        "Band Stamp Part Number A", "Die A Part Number","Die B Part Number","Die C Part Number","Die D Part Number",
        "Width", "Thickness", "Abutment Punch Data", "A Notches Removed", "B Notches Removed",
        "Tang Length Number"
    FROM BANDS
"""
# Set Some Constants
HOSTNAME = socket.gethostname()
BAND_COLUMNS = [
    'PartNumber', 'MatSpec', 'NumNotches', 'BandStampPnA', 'DiePnA', 'DiePnB',
    'DiePnC', 'DiePnD', 'Width', 'Thickness', 'AbutPunch', 'ANotchesRemoved',
    'BNotchesRemoved', 'TangLength'
]

BAND_DTYPE_MAP = {
    'PartNumber': str,
    'MatSpec': str,
    'NumNotches': float,
    'BandStampPnA': str,
    'DiePnA': str,
    'DiePnB': str,
    'DiePnC': str,
    'DiePnD': str,
    'Width': float,
    'Thickness': float,
    'AbutPunch': str,
    'ANotchesRemoved': float,
    'BNotchesRemoved': float,
    'TangLength': float,
}

BAND_NUMERIC_DEFAULTS = {
    'Width': 0.0,
    'Thickness': 0.0,
    'NumNotches': 0,
    'ANotchesRemoved': 0,
    'BNotchesRemoved': 0,
    'TangLength': 0,
}

PARTS_COLUMNS = [
    "PartNumber",
    "Band",
    "Housing",
    "Screw",
    "Feed",
    "DiaMax",
    "DiaMin",
    "HexSz",
    "BandThickness",
    "BandWidth",
    "CamInspect",
    "ScrDrvChk",
    "Cutout1",
    "Drawing",
    "Size",
    "Pack",
]

PARTS_DTYPE_MAP = {
    "PartNumber": str,
    "Band": str,
    "Housing": str,
    "Screw": str,
    "Feed": float,
    "DiaMax": float,
    "DiaMin": float,
    "HexSz": str,
    "BandThickness": float,
    "BandWidth": float,
    "CamInspect": str,
    "ScrDrvChk": str,
    "Cutout1": float,
    "Drawing": str,
    "Size": str,
    "Pack": str,
}

REQUIRED_NON_NONE_COLUMNS = [
    "Band",
    "Housing",
    "Screw",
    "HexSz",
    "CamInspect",
    "PartNumber",
]

NUMERIC_COLUMNS_TO_ROUND = [
    "Feed",
    "BandWidth",
    "BandThickness",
    "DiaMax",
    "DiaMin",
    "Cutout1",
]

HEX_SIZE_NORMALIZATION = {
    "Purchased 5/16": "5/16",
    "MX ONLY 10 mm ss": "10 mm",
    "7 mm Umbrella": "7 mm",
    "10 mm SS MX ONLY": "10 mm",
}

COMPONENTS_COLUMNS = ["ITMID", "QTY", "ITMDESC", "CLASS"]
COMPONENTS_DTYPE = {
    "ITMID": str,
    "QTY": int,
    "ITMDESC": str,
    "CLASS": str,
}
# File path constants
CSV_OUTPUT_PATH = 'C:\\Inetpub\\ftproot\\acmparts\\'  # Change to the appropriate output directory
if HOSTNAME == 'BNAWS625':
    CSV_OUTPUT_PATH = 'Y:\\Inetpub\\ftproot\\acmparts\\'  # Change to the appropriate output directory

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

def _build_components_dataframe(raw_records: list) -> pd.DataFrame:
    """
    Build the components inventory DataFrame from raw AS400 records.

    This function is responsible only for shaping, cleaning, and casting
    the data into the expected schema.
    """
    if not raw_records:
        # Return an empty DataFrame with the correct schema
        return pd.DataFrame(columns=COMPONENTS_COLUMNS)

    df_inv = pd.DataFrame.from_records(raw_records, columns=COMPONENTS_COLUMNS)

    # Drop any rows missing required fields before numeric conversion
    df_inv = df_inv.dropna(subset=COMPONENTS_COLUMNS)

    # Convert QTY to numeric, coercing invalid entries to NaN
    df_inv["QTY"] = pd.to_numeric(df_inv["QTY"], errors="coerce")

    # Remove rows where QTY could not be converted
    df_inv = df_inv.dropna(subset=["QTY"])

    try:
        df_inv = df_inv.astype(COMPONENTS_DTYPE)
    except (TypeError, ValueError) as exc:
        print(f"Error converting component inventory data types: {exc}")
        return pd.DataFrame(columns=COMPONENTS_COLUMNS)

    # Let pandas choose the most appropriate dtypes (nullable ints, etc.)
    df_inv = df_inv.convert_dtypes()

    print(f"Processed {len(df_inv)} component inventory records")
    return df_inv


def comp_df() -> pd.DataFrame:
    """
    Retrieve component inventory data from AS400 and return it as a cleaned DataFrame.
    """
    print("Getting component inventory data from AS400")
    raw_records = pull_data(CONNAS400_CCSDTA, sql_inv)

    if not raw_records:
        print("Warning: No component inventory data retrieved from AS400")
        return pd.DataFrame(columns=COMPONENTS_COLUMNS)

    return _build_components_dataframe(raw_records)

def parts_df() -> pd.DataFrame:
    """Build and clean Parts DataFrame from FileMaker source."""
    print("Getting Data From Filemaker")
    raw_data = pull_data(CONNFM, sql_parts)

    if not raw_data:
        print("Warning: No data retrieved from Filemaker")
        return pd.DataFrame()

    # Build initial DataFrame with explicit columns
    df_parts = pd.DataFrame.from_records(raw_data, columns=PARTS_COLUMNS)

    # Basic filtering and row slicing
    df_parts = df_parts[df_parts["Feed"] != "N/A"].iloc[1:].copy()

    # Clean string columns (trim & remove quotes)
    df_parts = _clean_string_columns(df_parts)

    # Convert to target dtypes
    try:
        df_parts = df_parts.astype(PARTS_DTYPE_MAP)
    except (TypeError, ValueError) as exc:
        print(f"Error converting data types: {exc}")
        return pd.DataFrame()

    # Filter out placeholder "None" values in required columns
    for column in REQUIRED_NON_NONE_COLUMNS:
        df_parts = df_parts[df_parts[column] != "None"]

    # Normalize case for inspection / screw-driver check flags
    df_parts["CamInspect"] = df_parts["CamInspect"].str.upper()
    df_parts["ScrDrvChk"] = df_parts["ScrDrvChk"].str.upper()

    # Fill numeric NaNs with 0.0 and round
    df_parts[NUMERIC_COLUMNS_TO_ROUND] = (
        df_parts[NUMERIC_COLUMNS_TO_ROUND].fillna(0.0).round(3)
    )

    # Normalize HexSz text variants
    df_parts["HexSz"] = df_parts["HexSz"].replace(HEX_SIZE_NORMALIZATION)

    # Final cleanup: drop remaining NaNs, normalize dtypes, deduplicate and sort
    df_parts = (
        df_parts.dropna()
        .convert_dtypes()
        .drop_duplicates(subset="PartNumber", keep="first")
        .sort_values(by="PartNumber")
    )

    print(f"Processed {len(df_parts)} records")
    return df_parts

def _clean_string_columns(df) -> pd.DataFrame:
    """
    Trim whitespace and remove quotes from all string columns in the given DataFrame.
    """
    string_cols = df.select_dtypes(include=['object']).columns
    if not len(string_cols):
        return df

    df[string_cols] = df[string_cols].apply(lambda col: col.str.strip())
    df[string_cols] = df[string_cols].apply(
        lambda col: col.str.replace(r'["\']', '', regex=True)
    )
    return df

def bands_df() -> pd.DataFrame:
    """
        Build and clean the Bands DataFrame from FileMaker data.

        Returns
        -------
        pandas.DataFrame
            Cleaned bands data, or an empty DataFrame if no data or errors occur.
        """
    print('Getting Data From Filemaker')
    data = pull_data(CONNFM, sql_bands)

    if not data:
        print("Warning: No data retrieved from Filemaker")
        return pd.DataFrame()

    df_bands = pd.DataFrame.from_records(data)
    df_bands.columns = BAND_COLUMNS

    # Clean string columns (trim + remove quotes)
    df_bands = _clean_string_columns(df_bands)

    # Basic filtering and cleansing
    df_bands = df_bands[df_bands['PartNumber'] != 'N/A']
    df_bands = df_bands.iloc[1:]  # skip header/invalid first row if present

    # Convert data types
    try:
        df_bands = df_bands.astype(BAND_DTYPE_MAP)
    except Exception as e:
        print(f"Error converting data types: {e}")
        return pd.DataFrame()

    # Round numeric columns
    df_bands['Width'] = df_bands['Width'].round(2)
    df_bands['Thickness'] = df_bands['Thickness'].round(3)
    df_bands['TangLength'] = df_bands['TangLength'].round(2)
    df_bands['NumNotches'] = df_bands['NumNotches'].round(0)

    # Fill missing numeric values with safe defaults
    df_bands.fillna(BAND_NUMERIC_DEFAULTS, inplace=True)

    # Final cleanup: drop any remaining NaNs, normalize dtypes, deduplicate and sort
    df_bands = df_bands.dropna()
    df_bands = df_bands.convert_dtypes()
    df_bands.drop_duplicates(subset='PartNumber', keep='first', inplace=True)
    df_bands = df_bands.sort_values(by='PartNumber')

    print(f"Processed {len(df_bands)} records")
    return df_bands

def part_tbl(df_data):
    """
        Build Part Table and insert data into SQL Server.

        Args:
            df_data: DataFrame containing band data

        Raises:
            ValueError: If DataFrame is empty or missing required columns
            Exception: If database operation fails
        """
    # Build Parts Table
    print('Build Part SQL Table')
    if df_data.empty:
        print("Warning: Empty DataFrame, skipping SQL insert")
        return

    # Validate required columns exist
    required_columns = ['PartNumber', 'Band', 'Housing', 'Screw', 'Feed', 'DiaMax',
                        'DiaMin', 'HexSz', 'BandThickness', 'BandWidth', 'CamInspect',
                        'ScrDrvChk', 'Cutout1', 'Drawing', 'Size']
    missing_columns = [col for col in required_columns if col not in df_data.columns]
    if missing_columns:
        error_msg = f"Missing required columns: {missing_columns}"
        print(f"Error: {error_msg}")
        raise ValueError(error_msg)

    data_type_dict = {'PartNumber': sqlalchemy.types.VARCHAR(255), 'Band': sqlalchemy.types.VARCHAR(255),
                      'Housing': sqlalchemy.types.VARCHAR(255), 'Screw': sqlalchemy.types.VARCHAR(255),
                      'Feed': sqlalchemy.types.Float, 'DiaMax': sqlalchemy.types.Float,
                      'DiaMin': sqlalchemy.types.Float, 'HexSz': sqlalchemy.types.VARCHAR(255),
                      'BandThickness': sqlalchemy.types.Float, 'BandWidth': sqlalchemy.types.Float,
                      'CamInspect': sqlalchemy.types.VARCHAR(255), 'ScrDrvChk': sqlalchemy.types.VARCHAR(255),
                      'Cutout1': sqlalchemy.types.Float, 'Drawing': sqlalchemy.types.VARCHAR(255),
                      'Size': sqlalchemy.types.VARCHAR(255), 'Pack': sqlalchemy.types.VARCHAR(255)}
    try:
        df_data.to_sql('parts_clamps', engine, schema='production', if_exists='replace', index=False,
                         dtype=data_type_dict)
        print(f"Successfully inserted {len(df_data)} records into parts_clamps")
    except Exception as e:
        print(f"Error inserting data into parts_clamps: {e}")
        raise  # Re-raise to allow calling code to handle the error

def band_tbl(df_data):
    """
    Build Band Table and insert data into SQL Server.

    Args:
        df_data: DataFrame containing band data

    Raises:
        ValueError: If DataFrame is empty or missing required columns
        Exception: If database operation fails
    """

    # Build Band Table
    print('Build Part SQL Table')

    if df_data.empty:
        print("Warning: Empty DataFrame, skipping SQL insert")
        return

    # Validate required columns exist
    required_columns = ['PartNumber', 'MatSpec', 'NumNotches', 'BandStampPnA',
                       'DiePnA', 'DiePnB', 'DiePnC', 'DiePnD', 'Width',
                       'Thickness', 'AbutPunch', 'ANotchesRemoved',
                       'BNotchesRemoved', 'TangLength']
    missing_columns = [col for col in required_columns if col not in df_data.columns]
    if missing_columns:
        error_msg = f"Missing required columns: {missing_columns}"
        print(f"Error: {error_msg}")
        raise ValueError(error_msg)

    data_type_dict = {'PartNumber': sqlalchemy.types.VARCHAR(255), 'MatSpec': sqlalchemy.types.VARCHAR(255),
                      'NumNotches': sqlalchemy.types.Float, 'BandStampPnA': sqlalchemy.types.VARCHAR(255),
                      'DiePnA': sqlalchemy.types.VARCHAR(255), 'DiePnB': sqlalchemy.types.VARCHAR(255),
                      'DiePnC': sqlalchemy.types.VARCHAR(255), 'DiePnD': sqlalchemy.types.VARCHAR(255),
                      'Width': sqlalchemy.types.Float, 'Thickness': sqlalchemy.types.Float,
                      'AbutPunch': sqlalchemy.types.VARCHAR(255), 'ANotchesRemoved': sqlalchemy.types.Float,
                      'BNotchesRemoved': sqlalchemy.types.Float, 'TangLength': sqlalchemy.types.Float}
    try:
        df_data.to_sql('tblBands', engine, schema='eng', if_exists='replace', index=False,
                         dtype=data_type_dict)
        print(f"Successfully inserted {len(df_data)} records into eng.tblBands")
    except Exception as e:
        print(f"Error inserting data into eng.tblBands: {e}")
        raise  # Re-raise the exception so caller can handle it

def comp_tbl(df_data):
    # Build Components Table
    print('Build Component SQL Table')
    if df_data.empty:
        print("Warning: Empty DataFrame, skipping SQL insert")
        return

    # Validate required columns exist
    required_columns = ['ITMID', 'QTY', 'ITMDESC', 'CLASS']
    missing_columns = [col for col in required_columns if col not in df_data.columns]
    if missing_columns:
        error_msg = f"Missing required columns: {missing_columns}"
        print(f"Error: {error_msg}")
        raise ValueError(error_msg)

    data_type_dict = {'ITMID': sqlalchemy.types.VARCHAR(255), 'QTY': sqlalchemy.types.INT,
                      'ITMDESC': sqlalchemy.types.VARCHAR(255),
                      'CLASS': sqlalchemy.types.VARCHAR(255)}
    try:
        df_data.to_sql('tblInvAll', engine, schema='production', if_exists='replace', index=False,
                         dtype=data_type_dict)
        print(f"Successfully inserted {len(df_data)} records into tblCompInv")
    except Exception as e:
        print(f"Error inserting data into tblCompInv: {e}")


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
        df_bands = bands_df()

        # Save to SQL Server
        part_tbl(df_parts)
        comp_tbl(df_components)
        band_tbl(df_bands)

        # Save to CSV files
        save_dataframe_to_csv(df_parts, 'parts_clamps.csv')
        # save_dataframe_to_csv(df_components, 'components_inventory.csv')

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