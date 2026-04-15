# coding=utf-8
#!/usr/bin/env python

"""Pandas Driven Data Updates to SQL Server. Will Eventually replace updates and getclamps"""
import csv
import pandas as pd
import as400
import sql_funcs
import common_funcs
import pyodbc
import os
import socket
import sqlalchemy.types
from timeit import default_timer as timer
from typing import Mapping, Hashable, Any

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

# Define Some SQL Statements


sql_parts = """
    SELECT Ourpart,"Band A Part Number", "Housing A Part Number",
        "Screw Part Number", "Band Feed from Band data",
        "Ship Diam Max", "Ship Diam Min", "Hex Size", "Band_Thickness", "Band_Width",
        "CameraInspectionRequired", "ScrDrvChk", "Cutout1", "DrawingPrintNumber","Size", "Pack", "Ultimate Torque Min"
    FROM tbl8Tridon
"""

sql_bands = """
    SELECT "Band Part Number","Material Spec Number", "Number of Notches",
        "Band Stamp Part Number A", "Die A Part Number","Die B Part Number","Die C Part Number","Die D Part Number",
        "Width", "Thickness", "Abutment Punch Data", "A Notches Removed", "B Notches Removed",
        "Tang Length Number", "Band Length", "Feed Length", "Dim A", "Dim B", "Dim C", "Dim D", "Die A Note", 
        "Die B Note", "Die C Note", "Die D Note"
    FROM BANDS
"""
# Set Some Constants
HOSTNAME = socket.gethostname()
BAND_COLUMNS = [
    'PartNumber', 'MatSpec', 'NumNotches', 'BandStampPnA', 'DiePnA', 'DiePnB',
    'DiePnC', 'DiePnD', 'Width', 'Thickness', 'AbutPunch', 'ANotchesRemoved',
    'BNotchesRemoved', 'TangLength', 'BandLength', 'FeedLength', 'DimA', 'DimB', 'DimC', 'DimD', 'DieANote',
    'DieBNote', 'DieCNote', 'DieDNote'
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
    'BandLength': float,
    'FeedLength': float,
    'DimA': float,
    'DimB': float,
    'DimC': float,
    'DimD': float,
    'DieANote': str,
    'DieBNote': str,
    'DieCNote': str,
    'DieDNote': str,
}

BAND_NUMERIC_DEFAULTS = {
    'Width': 0.0,
    'Thickness': 0.0,
    'NumNotches': 0,
    'ANotchesRemoved': 0,
    'BNotchesRemoved': 0,
    'TangLength': 0,
    'BandLength': 0,
    'FeedLength': 0,
    'DimA': 0,
    'DimB': 0,
    'DimC': 0,
    'DimD': 0,
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
    "UltimateTorqueMin",
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
    "UltimateTorqueMin": float,
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
    "UltimateTorqueMin",
]

HEX_SIZE_NORMALIZATION = {
    "Purchased 5/16": "5/16",
    "MX ONLY 10 mm ss": "10 mm",
    "7 mm Umbrella": "7 mm",
    "10 mm SS MX ONLY": "10 mm",
}

COMPONENTS_COLUMNS = ["ITMID", "QTY", "ITMDESC", "CLASS", "MAJLOC", "MINLOC"]
COMPONENTS_DTYPE = {
    "ITMID": str,
    "QTY": int,
    "ITMDESC": str,
    "CLASS": str,
    "MAJLOC": str,
    "MINLOC": str,
}
# File path constants
CSV_OUTPUT_PATH = 'C:\\Inetpub\\ftproot\\acmparts\\'  # Change to the appropriate output directory
if HOSTNAME == 'BNAWS625':
    CSV_OUTPUT_PATH = 'Y:\\Inetpub\\ftproot\\acmparts\\'  # Change to the appropriate output directory

def pull_data(conn,qry):
    """
    Pulls data from a database by executing a given SQL query on a specified connection string.

    This function establishes a connection to a database using the provided connection string,
    executes the supplied SQL query, and retrieves the results. It includes error handling for
    both connection and query execution failures, with optional mechanisms to send email
    notifications in case of errors.

    :param conn: A connection string used to connect to the database.
    :type conn: str
    :param qry: The SQL query to be executed on the database.
    :type qry: str
    :return: A list of query results fetched from the database.
    :rtype: list
    """
    # Connection with error handling and connection management
    result = []
    start = 0
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
            common_funcs.build_email2(
                body_data= msg,
                subject="SQL Query Failure",
                message_header="Query Failed for the Following reason:\n\n",
                mailto=sql_funcs.EMAIL_RECIPIENTS
            )
    except pyodbc.Error as e:
        msg = conn + ' Connection Failed: ' + str(e)
        print(msg)
        common_funcs.build_email2(
            body_data= msg,
            subject="SQL Connection Failure",
            message_header="Connection Failed for the Following reason:\n\n",
            mailto=sql_funcs.EMAIL_RECIPIENTS
        )
    finally:
        if msg:
            print(msg)
        if dbcnxn is not None:
            try:
                print("Connect/Query Time = " + str(round((timer() - start), 3)) + " sec")
                dbcnxn.close()
            except pyodbc.Error:
                pass  # Handle case where connection wasn't established

    return result

def _build_components_dataframe(raw_records: list) -> pd.DataFrame:
    """
    Build the dataframe containing the inventory of components from raw AS400 records.

    This function is responsible only for shaping, cleaning, and casting
    the data into the expected schema.
    """
    if not raw_records:
        # Return an empty DataFrame with the correct schema
        return pd.DataFrame(columns=COMPONENTS_COLUMNS)

    df_inv = pd.DataFrame.from_records(raw_records, columns=COMPONENTS_COLUMNS)

    # Drop any rows missing required fields before numeric conversion
    df_inv = df_inv.dropna(subset=COMPONENTS_COLUMNS)

    # Remove "00" from ITEMID if it starts with 8, is 7 chars long, and ends with 00
    mask = (
            df_inv["ITMID"].str.startswith("8") &
            (df_inv["ITMID"].str.len() == 7) &
            df_inv["ITMID"].str.endswith("00")
    )
    df_inv.loc[mask, "ITMID"] = df_inv.loc[mask, "ITMID"].str[:-2]

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


def comp_df(plant) -> pd.DataFrame:
    """
    Retrieves and processes component inventory data from an AS400 database.

    This function fetches raw component inventory data from an AS400 system using the
    provided plant number. If data is successfully retrieved, it is processed into a
    pandas DataFrame. If no data is retrieved, an empty DataFrame with predefined columns
    is returned.

    :param plant: Plant identifier for the component inventory data
    :return: A pandas DataFrame containing the processed component inventory data
    :rtype: pd.DataFrame
    """

    sql_inv = f"""
                 SELECT STRIP(y.itmid), \
                        b.qty, \
                        STRIP(y.itmdesc), \
                        STRIP(SUBSTR(Altdesc, 15, 1)) Class, \
                        STRIP(b.MAJLOC), \
                        STRIP(b.MINLOC)
                 FROM CCSDTA.DCSCIM y, \
                      CCSDTA.DMFCMAR x, \
                      CCSDTA.DCSILM b
                 WHERE x.itmid = y.itmid
                   AND x.itmid = b.itmid
                   AND x.plt = b.plt
                   AND b.plt = {plant}
                   AND qty <> 0
                   AND x.COSTID = 'FRZ'
                   AND x.plt NOT IN ('53', '54', '55', '56', '59') \
                 """

    print("Getting component inventory data from AS400 for plant: ", plant, "")
    raw_records = pull_data(CONNAS400_CCSDTA, sql_inv)

    if not raw_records:
        print("Warning: No component inventory data retrieved from AS400")
        return pd.DataFrame(columns=COMPONENTS_COLUMNS)

    return _build_components_dataframe(raw_records)

def parts_df() -> pd.DataFrame:
    """
    Transforms raw data retrieved from FileMaker into a structured and clean DataFrame of parts.

    This function pulls data from a FileMaker database using a predefined connection and SQL query.
    The raw data is processed through several cleaning, filtering, and normalization steps to
    generate a structured pandas DataFrame. It ensures the data adheres to specific formatting
    rules, handles missing or invalid values, and prepares it for further analysis or usage.

    :returns: A cleaned and processed pandas DataFrame containing parts information.
              If no valid data is available or errors occur during transformation,
              an empty DataFrame is returned.
    :rtype: pd.DataFrame
    """
    print("Getting Part Data From Filemaker")
    raw_data = pull_data(CONNFM, sql_parts)

    if not raw_data:
        print("Warning: No data retrieved from Filemaker")
        return pd.DataFrame()

    raw_data = tuple(map(lambda x: x, raw_data))
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

    # Normalize case for inspection / screwdriver check flags
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
    Cleans string columns in the given DataFrame by applying transformations such as stripping
    leading and trailing whitespace and removing specific characters like quotes.

    This function identifies all columns in the DataFrame with type `object`, processes
    them by removing unnecessary whitespace and quotes, and returns the cleaned DataFrame.
    If there are no string columns in the input DataFrame, the function returns the original
    DataFrame without any modifications.

    :param df: The input pandas DataFrame whose object-type columns need to be cleaned.
    :type df: pd.DataFrame
    :return: A pandas DataFrame with cleaned string columns. The original DataFrame is
        returned if no object-type columns are present.
    :rtype: pd.DataFrame
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
    print('Getting Band Data From Filemaker')
    data = pull_data(CONNFM, sql_bands)

    if not data:
        print("Warning: No data retrieved from Filemaker")
        return pd.DataFrame()

    data = tuple(map(lambda x: x, data))
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
    df_bands['FeedLength'] = df_bands['FeedLength'].round(3)
    df_bands['BandLength'] = df_bands['BandLength'].round(3)
    df_bands['DimA'] = df_bands['DimA'].round(3)
    df_bands['DimB'] = df_bands['DimB'].round(3)
    df_bands['DimC'] = df_bands['DimC'].round(3)
    df_bands['DimD'] = df_bands['DimD'].round(3)

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
                        'ScrDrvChk', 'Cutout1', 'Drawing', 'Size', 'Pack','UltimateTorqueMin']
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
                      'Size': sqlalchemy.types.VARCHAR(255), 'Pack': sqlalchemy.types.VARCHAR(255),
                      'UltimateTorqueMin': sqlalchemy.types.Float}
    try:
        df_data.to_sql('parts_clamps', as400.engine, schema='production', if_exists='replace', index=False,
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
                       'BNotchesRemoved', 'TangLength', 'BandLength', 'FeedLength',
                        'DimA', 'DimB', 'DimC', 'DimD', 'DieANote', 'DieBNote', 'DieCNote', 'DieDNote']
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
                      'BNotchesRemoved': sqlalchemy.types.Float, 'TangLength': sqlalchemy.types.Float,
                      'BandLength': sqlalchemy.types.Float, 'FeedLength': sqlalchemy.types.Float,
                      'DimA': sqlalchemy.types.Float, 'DimB': sqlalchemy.types.Float,
                      'DimC': sqlalchemy.types.Float, 'DimD': sqlalchemy.types.Float,
                      'DieANote': sqlalchemy.types.VARCHAR(255), 'DieBNote': sqlalchemy.types.VARCHAR(255),
                      'DieCNote': sqlalchemy.types.VARCHAR(255), 'DieDNote': sqlalchemy.types.VARCHAR(255)}
    try:
        df_data.to_sql('tblBands', as400.engine, schema='eng', if_exists='replace', index=False,
                         dtype=data_type_dict)
        print(f"Successfully inserted {len(df_data)} records into eng.tblBands")
    except Exception as e:
        print(f"Error inserting data into eng.tblBands: {e}")
        raise  # Re-raise the exception so caller can handle it

def comp_tbl(df_data, tbl_name):
    """
    Builds a component SQL table from the provided DataFrame. Validates necessary columns, converts
    data types where applicable, and inserts the data into the specified SQL table. Stops execution
    if the DataFrame is empty or required columns are missing.

    :param df_data: The DataFrame containing component data to be processed. It must include the
        following required columns: 'ITMID', 'QTY', 'ITMDESC', and 'CLASS'. Extra columns will be
        handled if they are included in `data_type_dict`.
    :type df_data: pandas.DataFrame

    :param tbl_name: The name of the SQL table where the data should be inserted.
    :type tbl_name: str

    :return: None
    """

    # Build Components Table
    print('Building Component SQL Table ' + tbl_name)

    if df_data is None:
        raise ValueError("df_data cannot be None")


    if df_data.empty:
        print("Warning: Empty DataFrame, skipping SQL insert")
        return

    # Validate required columns exist
    required_columns = ['ITMID', 'QTY', 'ITMDESC', 'CLASS', 'MAJLOC', 'MINLOC']
    missing_columns = [col for col in required_columns if col not in df_data.columns]
    if missing_columns:
        error_msg = f"Missing required columns: {missing_columns}"
        print(f"Error: {error_msg}")
        raise ValueError(error_msg)

    data_type_dict: Mapping[Hashable, Any] = {'ITMID': sqlalchemy.types.VARCHAR(255),
                                              'QTY': sqlalchemy.types.INT,
                                              'ITMDESC': sqlalchemy.types.VARCHAR(255),
                                              'CLASS': sqlalchemy.types.VARCHAR(255),
                                              'MAJLOC': sqlalchemy.types.VARCHAR(255),
                                              'MINLOC': sqlalchemy.types.VARCHAR(255)
                                              }


    try:
        df_data.to_sql(tbl_name, as400.engine, schema='production', if_exists='replace', index=False, dtype=data_type_dict)
        print(f"Successfully inserted {len(df_data)} records into {tbl_name}")
    except Exception as e:
        print(f"Error inserting data into {tbl_name}: {e}")
        raise


def save_dataframe_to_csv(df_data, filename, output_path=CSV_OUTPUT_PATH):
    """
    Save DataFrame to the CSV file.

    Args:
        df_data: DataFrame to save
        filename: Name of the CSV file (without the path)
        output_path: Directory path where the file will be saved

    Returns:
        str: Full path of the saved file or None on error
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


def get_orders():
    """
    Fetches and updates orders data from multiple sources.

    This function interacts with external systems to retrieve orders
    and then updates them in the database. It performs the following
    actions sequentially:
      - Retrieves current orders and updates them in the database.
      - Retrieves all completed orders and updates them in the database.

    :return: None
    """
    # Main Function
    orders = as400.get_orders()
    sql_funcs.update_orders(orders)

    all_orders = as400.get_comp_orders()
    sql_funcs.update_all_orders(all_orders)
    return

def main():
    """
    Executes the main application flow required to manage and persist data for parts,
    components, and bands. The process involves fetching required data, processing it,
    and saving the results into both databases and CSV files, while handling potential
    errors and ensuring cleanup.

    :raises Exception: If an error occurs during the execution of the main workflow.
    :return: None
    """
    try:
        # Get Orders
        get_orders()

        # Get component data
        df_parts = parts_df()
        df_bands = bands_df()
        df_comp_03 = comp_df('03')
        df_comp_06 = comp_df('06')
        df_comp_08 = comp_df('08')
        df_comp_09 = comp_df('09')


        # Save to SQL Server
        part_tbl(df_parts)
        band_tbl(df_bands)
        comp_tbl(df_comp_09, 'tblInv09')
        comp_tbl(df_comp_09, 'tblInvAll')
        comp_tbl(df_comp_03, 'tblInv03')
        comp_tbl(df_comp_06, 'tblInv06')
        comp_tbl(df_comp_08, 'tblInv08')


        # Save to CSV files
        save_dataframe_to_csv(df_parts, 'parts_clamps.csv')
        # save_dataframe_to_csv(df_components, 'components_inventory.csv')

    except Exception as e:
        print(f"Error in main: {e}")
    finally:
        # Close the connection
        if 'conn_sql' in globals():
            as400.conn_sql.close()
            as400.engine.dispose()
    return

if __name__ == '__main__':
    main()