# coding=utf-8
# !/usr/bin/env python

"""Common Database Functions Used on the iSeries AS400"""

import pyodbc
import logging
import common_funcs
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Define Database Connection

CONNAS400 = """
Driver={iSeries Access ODBC Driver};
system=10.143.12.10;
Server=AS400;
Database=PROD;
UID=SMY;
PWD=SMY;
"""
TABLE_FPSPRMAST1 = "PROD.FPSPRMAST1"
TABLE_FPSPRMAST2 = "PROD.FPSPRMAST2"
TABLE_DMFMOMRID1 = "PROD.DMFMOMRID1"
FACILITY = 9
WEEK_NUMBER = common_funcs.get_custom_week_number(datetime.today())


def connect_to_db():
    """Establish a Database Connection."""
    try:
        return pyodbc.connect(CONNAS400)
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
    database = []
    for row in result:
        row[10] = make_date(row[10])
        database.append([str(x) for x in row])
    return database



def get_usage():
    """Get Spare Part Usage Data From iSeries AS400."""
    # tables = ('FPSPRUSAG', 'FPSPRUSAGC', 'FPSPRUSAGP', 'FPSPRUSAGS')
    tables = ['FPSPRUSAG']
    database = []

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
                database.append([str(x) for x in row])
    return database

def get_prod():
    """Get Production Data From iSeries AS400."""
    tables = ['FPMGFILE']
    database = []

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
                database.append([str(x) for x in row])
    return database

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

