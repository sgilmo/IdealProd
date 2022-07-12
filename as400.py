#!/usr/bin/env python

"""Common Database Functions Used on the iSeries AS400"""

import pyodbc

# Define Database Connection

CONNAS400 = """
Driver={iSeries Access ODBC Driver};
system=10.143.12.10;
Server=AS400;
Database=PROD;
UID=SMY;
PWD=SMY;
"""


def get_inv():
    """Get Spare Inventory Data From iSeries AS400"""
    dbcnxn = pyodbc.connect(CONNAS400)
    cursor = dbcnxn.cursor()
    dbase = []

    strsql = """SELECT PROD.FPSPRMAST1.SPH_PART,
                       STRIP(PROD.FPSPRMAST1.SPH_ENGPRT),
                       STRIP(PROD.FPSPRMAST1.SPH_DESC1),
                       STRIP(PROD.FPSPRMAST1.SPH_DESC2),
                       STRIP(PROD.FPSPRMAST1.SPH_MFG),
                       STRIP(PROD.FPSPRMAST1.SPH_MFGPRT),
                       STRIP(PROD.FPSPRMAST2.SPD_CABINT),
                       STRIP(PROD.FPSPRMAST2.SPD_DRAWER),
                       PROD.FPSPRMAST2.SPD_QOHCUR,
                       PROD.FPSPRMAST1.SPH_CURSTD,
                       STRIP(PROD.FPSPRMAST2.SPD_REODTE),
                       STRIP(PROD.FPSPRMAST2.SPD_USECC),
                       STRIP(PROD.FPSPRMAST2.SPD_PURCC)
                FROM PROD.FPSPRMAST1 INNER JOIN PROD.FPSPRMAST2 ON PROD.FPSPRMAST1.SPH_PART = PROD.FPSPRMAST2.SPD_PART
                WHERE (((PROD.FPSPRMAST2.SPD_FACIL)=9))"""
    try:
        cursor.execute(strsql)
        result = cursor.fetchall()
    except Exception as e:
        msg = 'AS400 Inventory Query Failed: ' + str(e)
        result = []
        print(msg)
        print(strsql)
    else:
        msg = str(len(result)) + " AS400 Inventory Records Processed From Inventory Tables"
        print(msg)
    for row in result:
        row[10] = make_date(row[10])
        dbase.append(list([str(x) for x in row]))
    dbcnxn.close()
    return dbase


def get_usage():
    """Get Spare Part Usage Data From iSeries AS400"""
    dbcnxn = pyodbc.connect(CONNAS400)
    cursor = dbcnxn.cursor()
    tables = ('FPSPRUSAG', 'FPSPRUSAGC', 'FPSPRUSAGP')
    dbase = []
    for table in tables:
        strsql = f"""-- noinspection SqlResolveForFile
                    SELECT STRIP(PROD.{table!s}.SPU_TRANDT),
                           STRIP(PROD.{table!s}.SPU_PART),
                           STRIP(PROD.{table!s}.SPU_ENGPRT),
                           STRIP(PROD.{table!s}.SPU_USECC),
                           STRIP(PROD.{table!s}.SPU_CLOCK),
                           STRIP(PROD.{table!s}.SPU_RIDPO),
                           PROD.{table!s}.SPU_TRNQTY,
                           PROD.{table!s}.SPU_STDCST,
                           ROUND(PROD.{table!s}.SPU_TRNQTY * PROD.{table!s}.SPU_STDCST,2)
                    FROM PROD.{table!s}
                    WHERE (PROD.{table!s}.SPU_FACIL = 09)
                    AND STRIP(PROD.{table!s}.SPU_USECC) IS NOT NULL"""
        try:
            cursor.execute(strsql)
            result = cursor.fetchall()
        except Exception as e:
            msg = 'AS400 Usage Query Failed: ' + str(e)
            result = []
            print(msg)
        else:
            msg = str(len(result)) + " AS400 Usage Records Processed From Table " + table
            print(msg)
        for row in result:
            row[0] = make_date(row[0])
            dbase.append(list([str(x) for x in row]))
    dbcnxn.close()
    return dbase


def get_emps():
    """Get Employees From iSeries AS400"""
    dbcnxn = pyodbc.connect(CONNAS400)
    cursor = dbcnxn.cursor()
    englogin = [9999, 'ELMER J FUDD', 'ENG']
    eng = ['9107', '1656', '1472', '1626', '1351']

    strsql = """SELECT STRIP(EMP_CLOCK_NUMBER) As Clock,
                    CONCAT(CONCAT(STRIP(EMP_FIRST_NAME), ' '),
                    STRIP(EMP_LAST_NAME)) As Name,
                    STRIP(EMP_POSITION_CODE) As Code
            FROM PROD.FPCLCKPAY
            WHERE (EMP_LOCATION = 09) AND (EMP_LAST_NAME <> 'TEMP') AND (EMP_SHIFT_TYPE = 'A')
            ORDER BY EMP_CLOCK_NUMBER"""
    try:
        cursor.execute(strsql)
        result = cursor.fetchall()
    except Exception as e:
        msg = 'AS400 Employee Query Failed: ' + str(e)
        result = []
        print(msg)
    else:
        msg = str(len(result)) + " AS400 Employee Records Processed From Table"
        print(msg)
    result.append(englogin)
    for row in result:
        if row[0] in eng:
            row[2] = 'ENG'
    dbcnxn.close()
    return result


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


