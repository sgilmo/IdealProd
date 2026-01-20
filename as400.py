# coding=utf-8
# !/usr/bin/env python

"""Common Database Functions Used on the iSeries AS400"""

import pandas as pd
import pyodbc
import sqlalchemy.types
from sqlalchemy import create_engine
from urllib import parse
from timeit import default_timer as timer
from datetime import datetime, timedelta
import logging
import common_funcs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Define Database Connection for PROD
CONNAS400_PROD = """
Driver={iSeries Access ODBC Driver};
system=10.143.12.10;
Server=AS400;
Database=PROD;
UID=SMY;
PWD=SMY;
"""

# Define Database Connection for CCSDTA
CONNAS400_CCSDTA = """
Driver={iSeries Access ODBC Driver};
system=10.143.12.10;
Server=AS400;
Database=CCSDTA;
UID=SMY;
PWD=SMY;
"""

# Set up Database Connection to SQL Server
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


# Define some globals
TABLE_FPSPRMAST1 = "PROD.FPSPRMAST1"
TABLE_FPSPRMAST2 = "PROD.FPSPRMAST2"
TABLE_DMFMOMRID1 = "PROD.DMFMOMRID1"
FACILITY = 9
WEEK_NUMBER = common_funcs.get_custom_week_number(datetime.today())

def date_to_julian(date_obj):
    """
    Convert a Python date/datetime to AS400 Julian format (YYDDD).

    Format: YYDDD where YY=year, DDD=day of year
    Example: January 1, 2026 -> 26001

    Args:
        date_obj: datetime.date, datetime.datetime, or string in 'YYYY-MM-DD' format

    Returns:
        int: Julian date in YYDDD format
    """
    if isinstance(date_obj, str):
        date_obj = datetime.strptime(date_obj, '%Y-%m-%d').date()
    elif isinstance(date_obj, datetime):
        date_obj = date_obj.date()

    year = date_obj.year
    year_digits = year % 100
    day_of_year = date_obj.timetuple().tm_yday

    julian = year_digits * 1000 + day_of_year
    return julian


def julian_to_date(julian_date):
    """
    Convert 5-digit Julian date (YYDDD) to yyyy-mm-dd format.

    Format: YYDDD where YY=year (assumes the 2000s), DDD=day of year
    Example: 26001 -> 2026-01-01

    Args:
        julian_date: int or str, 5-digit Julian date

    Returns:
        str: Date in 'YYYY-MM-DD' format
    """

    julian_str = str(int(julian_date)).zfill(5)
    year_digits = int(julian_str[:2])
    day_of_year = int(julian_str[2:])

    # Assume years 00-99 map to 2000-2099
    year = 2000 + year_digits

    # Start from January 1 of that year and add days
    base_date = datetime(year, 1, 1).date()
    target_date = base_date + timedelta(days=day_of_year - 1)

    return target_date.strftime('%Y-%m-%d')


# noinspection PyBroadException
def pull_data(conn,qry):
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
                pass  # Handle case where the connection wasn't established

    return result

def _build_scrap_dataframe(raw_records: list) -> pd.DataFrame:
    """
    Build the scrap DataFrame from raw AS400 records.

    This function is responsible only for shaping, cleaning, and casting
    the data into the expected schema.
    """

    # Set Column names and data types for scrap DataFrame
    scrap_columns = ["ITMID", "ITMDESC", "PRODCODE", "INVCLASS", "TXNCD", "REASON", "PLT", "TXNQTY", "UOM", "ITMCOST", "FRZSTDCST", "SCRAP_COST", "TNREF1", "TXNSDT", "DATE1", "USERID"]

    scrap_dtype = {
        "ITMID": str,
        "ITMDESC": str,
        "PRODCODE": str,
        "INVCLASS": str,
        "TXNCD": str,
        "REASON": str,
        "PLT": str,
        "TXNQTY": float,
        "UOM": str,
        "ITMCOST": float,
        "FRZSTDCST": float,
        "SCRAP_COST": float,
        "TNREF1": str,
        "TXNSDT": float,
        "DATE1": str,
        "USERID": str
    }


    if not raw_records:
        # Return an empty DataFrame with the correct schema
        return pd.DataFrame(columns=scrap_columns)

    # Create the dataframe with columns from the SQL query (excluding DATE1)
    sql_columns = ["ITMID", "ITMDESC", "PRODCODE", "INVCLASS", "TXNCD", "REASON", "PLT", "TXNQTY", "UOM", "ITMCOST", "FRZSTDCST", "SCRAP_COST", "TNREF1", "TXNSDT", "USERID"]
    df_scrap = pd.DataFrame.from_records(raw_records, columns=sql_columns)

    # Drop any rows missing required fields before numeric conversion
    df_scrap = df_scrap.dropna(subset=sql_columns)

    # Convert QTY to numeric, coercing invalid entries to NaN
    df_scrap["TXNQTY"] = pd.to_numeric(df_scrap["TXNQTY"], errors="coerce")

    # Remove rows where QTY could not be converted
    df_scrap = df_scrap.dropna(subset=["TXNQTY"])

    # Convert ITMCOST to numeric, coercing invalid entries to NaN
    df_scrap["ITMCOST"] = pd.to_numeric(df_scrap["ITMCOST"], errors="coerce")

    # Remove rows where ITMCOST could not be converted
    df_scrap = df_scrap.dropna(subset=["ITMCOST"])

    # Convert SCRAP_COST to numeric, coercing invalid entries to NaN
    df_scrap["SCRAP_COST"] = pd.to_numeric(df_scrap["SCRAP_COST"], errors="coerce")

    # Remove rows where SCRAP_COST could not be converted
    df_scrap = df_scrap.dropna(subset=["SCRAP_COST"])

    # Negate TXNQTY for transaction code 12
    df_scrap.loc[df_scrap["TXNCD"] == "12", "TXNQTY"] = df_scrap["TXNQTY"] * -1

    # Convert TXNSDT Julian date to yyyy-mm-dd format and add to new column DATE1
    df_scrap["DATE1"] = df_scrap["TXNSDT"].astype(int).apply(julian_to_date)

    try:
        df_scrap = df_scrap.astype(scrap_dtype)
    except (TypeError, ValueError) as exc:
        print(f"Error converting scrap data types: {exc}")
        return pd.DataFrame(columns=scrap_columns)

    # Let pandas choose the most appropriate dtypes (nullable ints, etc.)
    df_scrap = df_scrap.convert_dtypes()

    print(f"Processed {len(df_scrap)} scrap records")
    return df_scrap

def scrap_tbl(df_data):
    # Build Components Table
    print('Build Scrap SQL Table')
    if df_data.empty:
        print("Warning: Empty DataFrame, skipping SQL insert")
        return

    # Validate required columns exist
    required_columns = ["ITMID", "ITMDESC", "PRODCODE", "INVCLASS", "TXNCD", "REASON", "PLT", "TXNQTY", "UOM", "ITMCOST", "FRZSTDCST", "SCRAP_COST", "TNREF1", "TXNSDT", "DATE1", "USERID"]
    missing_columns = [col for col in required_columns if col not in df_data.columns]
    if missing_columns:
        error_msg = f"Missing required columns: {missing_columns}"
        print(f"Error: {error_msg}")
        raise ValueError(error_msg)

    data_type_dict = {
        "ITMID": sqlalchemy.types.VARCHAR(255),
        "ITMDESC": sqlalchemy.types.VARCHAR(255),
        "PRODCODE": sqlalchemy.types.VARCHAR(255),
        "INVCLASS": sqlalchemy.types.VARCHAR(255),
        "TXNCD": sqlalchemy.types.VARCHAR(255),
        "REASON": sqlalchemy.types.VARCHAR(255),
        "PLT": sqlalchemy.types.VARCHAR(255),
        "TXNQTY": sqlalchemy.types.FLOAT,
        "ITMCOST": sqlalchemy.types.FLOAT,
        "FRZSTDCST": sqlalchemy.types.FLOAT,
        "SCRAP_COST": sqlalchemy.types.FLOAT,
        "TNREF1": sqlalchemy.types.VARCHAR(255),
        "TXNSDT": sqlalchemy.types.FLOAT,
        "UOM": sqlalchemy.types.VARCHAR(255),
        "DATE1": sqlalchemy.types.Date,
        "USERID": sqlalchemy.types.VARCHAR(255)
    }
    try:
        df_data.to_sql('tblScrapAll', engine, schema='eng', if_exists='replace', index=False,
                         dtype=data_type_dict)
        print(f"Successfully inserted {len(df_data)} records into tblScrapAll")
    except Exception as e:
        print(f"Error inserting data into tblScrapAll: {e}")
    return

def build_scrap_table():

    current_year = datetime.now().year
    start_date = datetime(current_year, 1, 1)
    stop_date = datetime.today()
    scrap_start = date_to_julian(start_date)
    scrap_stop = date_to_julian(stop_date)

    sql_scrap = f"""
        SELECT
            x.itmid,
            y.itmdesc,
            substr(z.altdesc,12,1) PRODCODE,
            substr(z.altdesc,15,1) INVCLASS,
            txncd,
            reason,
            x.plt,
            txnqty,
            x.uom,
            x.itmcost,
            y.frzstdcst,
            ((y.frzstdcst/1000) * txnqty) AS SCRAP_COST,
            tnref1,
            txnsdt,
            userid
        FROM
            CCSDTA.DCSHST x,
            CCSDTA.DCSDIM y,
            CCSDTA.DCSCIM z
        WHERE
            x.plt = y.plt
            AND x.itmid = y.itmid
            AND x.itmid = z.itmid
            AND txnsdt > {scrap_start}
            AND txnsdt < {scrap_stop}
            AND y.plt = '09'
            AND reason not in ('790', '799', 'ERR', 'OP')
            AND substr(z.altdesc,12,1) not in ('7', 'C', 'D')
            AND tnref1 not in ('PIV0099', 'RMAD303522', 'RMAD303565')
            AND txncd in ('12', '13')
    """
    data_set = pull_data(CONNAS400_CCSDTA,sql_scrap)
    df_scrap = _build_scrap_dataframe(data_set)
    scrap_tbl(df_scrap)
    return

def connect_to_db():
    """Establish a Database Connection."""
    try:
        return pyodbc.connect(CONNAS400_PROD)
    except pyodbc.Error as ex:
        print(f"Database connection failed: {ex}")
        logger.error(f"Database connection failed: {ex}")
        raise


def process_query_result(cursor, query_sql, description):
    """Execute a query and return the result with error handling."""
    try:
        cursor.execute(query_sql)
        results = cursor.fetchall()
        print(f"{len(results)} {description} Processed.")
        return results
    except Exception as e:
        print(f"{description} Query Failed: {e}")
        logger.error(f"{description} Query Failed: {e}")
        return []


def get_orders():
    """Get Order Data From iSeries AS400."""
    query_sql = f"""
        SELECT STRIP({TABLE_DMFMOMRID1}.MFMOMR02),
               STRIP({TABLE_DMFMOMRID1}.MFMOMR03),
               STRIP({TABLE_DMFMOMRID1}.MFMOMR0C),
               STRIP({TABLE_DMFMOMRID1}.MFMOMR0I)
        FROM {TABLE_DMFMOMRID1}
        WHERE {TABLE_DMFMOMRID1}.MFMOMR01 = '09' AND LEFT({TABLE_DMFMOMRID1}.MFMOMR0I,1) = '3'
    """
    with connect_to_db() as db_connection:
        cursor = db_connection.cursor()
        return process_query_result(cursor, query_sql, "AS400 Order Records")

def get_comp_orders():
    """Get Order Data From iSeries AS400."""
    query_sql = f"""
        SELECT STRIP({TABLE_DMFMOMRID1}.MFMOMR02),
               STRIP({TABLE_DMFMOMRID1}.MFMOMR03),
               STRIP({TABLE_DMFMOMRID1}.MFMOMR0C),
               STRIP({TABLE_DMFMOMRID1}.MFMOMR0I)
        FROM {TABLE_DMFMOMRID1}
        WHERE {TABLE_DMFMOMRID1}.MFMOMR01 = '09'
    """
    with connect_to_db() as db_connection:
        cursor = db_connection.cursor()
        return process_query_result(cursor, query_sql, "AS400 Order Records")


def get_inv():
    """Get Spare Inventory Data From iSeries AS400."""
    query_sql = f"""
        SELECT {TABLE_FPSPRMAST1}.SPH_PART,
               STRIP({TABLE_FPSPRMAST1}.SPH_ENGPRT),
               STRIP({TABLE_FPSPRMAST1}.SPH_DESC1),
               STRIP({TABLE_FPSPRMAST1}.SPH_DESC2),
               STRIP({TABLE_FPSPRMAST1}.SPH_MFG),
               STRIP({TABLE_FPSPRMAST1}.SPH_MFGPRT),
               STRIP({TABLE_FPSPRMAST2}.SPD_CABINT),
               STRIP({TABLE_FPSPRMAST2}.SPD_DRAWER),
               {TABLE_FPSPRMAST2}.SPD_QOHCUR,
               {TABLE_FPSPRMAST1}.SPH_CURSTD,
               STRIP({TABLE_FPSPRMAST2}.SPD_REODTE),
               STRIP({TABLE_FPSPRMAST2}.SPD_USECC),
               STRIP({TABLE_FPSPRMAST2}.SPD_PURCC),
               STRIP({TABLE_FPSPRMAST2}.SPD_QREORD)
        FROM {TABLE_FPSPRMAST1}
        INNER JOIN {TABLE_FPSPRMAST2}
        ON {TABLE_FPSPRMAST1}.SPH_PART = {TABLE_FPSPRMAST2}.SPD_PART
        WHERE {TABLE_FPSPRMAST2}.SPD_FACIL = {FACILITY}
    """
    with connect_to_db() as db_connection:
        cursor = db_connection.cursor()
        return process_query_result(cursor, query_sql, "AS400 Inventory Records")



def build_inv_list(result):
    """Build Inventory List with Processed Data."""
    inventory_list = []
    for row in result:
        row[10] = make_date(row[10])
        inventory_list.append([str(x) for x in row])
    return inventory_list



def get_usage():
    """Get Spare Part Usage Data From iSeries AS400."""
    # tables = ('FPSPRUSAG', 'FPSPRUSAGC', 'FPSPRUSAGP', 'FPSPRUSAGS')
    tables = ['FPSPRUSAG']
    usage_list = []

    with connect_to_db() as db_connection:
        cursor = db_connection.cursor()
        for table in tables:
            query_sql = f"""
                SELECT STRIP(PROD.{table}.SPU_TRANDT),
                       STRIP(PROD.{table}.SPU_PART),
                       STRIP(PROD.{table}.SPU_ENGPRT),
                       STRIP(PROD.{table}.SPU_USECC),
                       STRIP(PROD.{table}.SPU_PURCC),
                       STRIP(PROD.{table}.SPU_CLOCK),
                       STRIP(PROD.{table}.SPU_RIDPO),
                       PROD.{table}.SPU_TRNQTY,
                       PROD.{table}.SPU_STDCST,
                       ROUND(PROD.{table}.SPU_TRNQTY * PROD.{table}.SPU_STDCST, 2)
                FROM PROD.{table}
                WHERE PROD.{table}.SPU_FACIL = {FACILITY}
                AND STRIP(PROD.{table}.SPU_USECC) IS NOT NULL
            """
            result = process_query_result(cursor, query_sql, f"AS400 Usage Records from {table}")
            for row in result:
                row[0] = make_date(row[0])
                usage_list.append([str(x) for x in row])
    return usage_list

def get_prod():
    """Get Production Data From iSeries AS400."""
    tables = ['FPMGFILE']
    prod_list = []
    with connect_to_db() as db_connection:
        cursor = db_connection.cursor()
        for table in tables:
            query_sql = f"""
                    SELECT PROD.{table}.IDEB_WEEK,
                           STRIP(PROD.{table}.IDEB_DAY),
                           PROD.{table}.IDEB_DEPT,
                           PROD.{table}.IDEB_EMP_NBR,
                           PROD.{table}.IDEB_SHIFT,
                           STRIP(PROD.{table}.IDEB_MACH_NBR),
                           STRIP(PROD.{table}.IDEB_PART),
                           PROD.{table}.IDEB_TICKET_NBR,
                           PROD.{table}.IDEB_TOTAL_QTY,
                           PROD.{table}.IDEB_STANDARD,
                           PROD.{table}.IDEB_ACTUAL_HOURS,
                           PROD.{table}.IDEB_OVERTIME_HOURS,
                           PROD.{table}.IDEB_MONTH
                    FROM PROD.{table}
                    WHERE PROD.{table}.IDEB_LOC = {FACILITY}
                    AND (PROD.{table}.IDEB_WEEK >= {WEEK_NUMBER} - 3 OR {WEEK_NUMBER} <= 3)
                    AND PROD.{table}.IDEB_TOTAL_QTY > 0
                """
            result = process_query_result(cursor, query_sql, f"AS400 Usage Records from {table}")
            for row in result:
                prod_list.append([str(x) for x in row])
    return prod_list

def get_emps():
    """Get Employees From iSeries AS400."""
    employees_eng_login = ['9999', 'ELMER J FUDD', 'ENG']
    engineers = {'1720', '1626', '9999', '2126', '1496', '2241'}

    query_sql = """
        SELECT STRIP(EMP_CLOCK_NUMBER) AS Clock,
               CONCAT(CONCAT(STRIP(EMP_FIRST_NAME), ' '),
               STRIP(EMP_LAST_NAME)) AS Name,
               STRIP(EMP_POSITION_CODE) AS Code
        FROM PROD.FPCLCKPAY
        WHERE EMP_LOCATION = '09'
        AND EMP_LAST_NAME <> 'TEMP'
        AND EMP_SHIFT_TYPE = 'A'
        ORDER BY EMP_CLOCK_NUMBER
    """
    try:
        with connect_to_db() as db_connection:
            cursor = db_connection.cursor()
            result = process_query_result(cursor, query_sql, "AS400 Employee Records")
            result.extend([employees_eng_login])
            for row in result:
                row[2] = 'ENG' if row[0] in engineers else 'DEF'
            return result
    except pyodbc.Error as ex:
        print("Unable to connect to SQL Server" if ex.args[0] == '08001' else f"An error occurred: {ex}")
        logger.error("Unable to connect to SQL Server" if ex.args[0] == '08001' else f"An error occurred: {ex}")
        return []



def make_date(val):
    """ Convert Strange AS400 Date Format to Something We Can Use"""
    dt = mid(val, 2, 2) + '/' + right(val, 2) + '/' + left(val, 2)
    return dt


def left(s, amount):
    """Emulate Left() String Function"""
    return s[:amount]


def right(s, amount):
    """Emulate Right() String Function"""
    return s[-amount:]


def mid(s, offset, amount):
    """Emulate Mid() String Function"""
    return s[offset:offset+amount]

