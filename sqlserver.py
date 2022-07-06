#!/usr/bin/env python

"""Common Database Functions Used on the SQL Server"""

import pyodbc

# Define Database Connection


CONNSQL = """
Driver={SQL Server Native Client 11.0};
Server=tn-sql14;
Database=autodata;
UID=production;
PWD=Auto@matics;
"""


def update_dbusage(dbase):
    """ Add Spare Part Usage Data to SQL server database"""
    print(len(dbase))
    dbcnxn = pyodbc.connect(CONNSQL)
    cursor = dbcnxn.cursor()
    cursor.fast_executemany = True
    # Delete Existing Records
    print("Deleting Existing Records on SQL Server")
    cursor.execute("TRUNCATE TABLE dbo.tblUsage")
    dbcnxn.commit()

    # Load spare part usage data onto SQL server
    print("Loading data to SQL server")
    strsql = """INSERT INTO dbo.tblUsage (Date,Part,EngPart,Dept,Clock,Machine,Qty,Cost,SubTotal) 
                VALUES (?,?,?,?,?,?,?,?,?)"""
    try:
        cursor.executemany(strsql, dbase)
        dbcnxn.commit()
        print(str(len(dbase)) + " Records Processed")
    except Exception as e:
        msg = 'SQL Query Failed: ' + str(e)
        print(msg)
    return


def update_emps(dbase):
    """Add Employee data to SQL server database"""
    dbcnxn = pyodbc.connect(CONNSQL)
    cursor = dbcnxn.cursor()
    cursor.fast_executemany = True
    # Delete Existing Records
    print("Deleting Existing Employee Records on SQL Server")
    cursor.execute("TRUNCATE TABLE production.EMPLOYEE")
    dbcnxn.commit()

    # Load Employee data onto SQL server
    print("Loading Employee data to SQL server")
    strsql = """INSERT INTO production.EMPLOYEE (ID,NAME,ROLE) 
                    VALUES (?,?,?)"""
    try:
        cursor.executemany(strsql, dbase)
        dbcnxn.commit()
        print(str(len(dbase)) + " Records Processed")
    except Exception as e:
        msg = 'SQL Inventory Query Failed: ' + str(e)
        print(msg)
    return


def update_dbinv(dbase):
    """ Add Spare Part Inventory Data to SQL server database"""
    print(len(dbase))
    dbcnxn = pyodbc.connect(CONNSQL)
    cursor = dbcnxn.cursor()
    cursor.fast_executemany = True
    # Delete Existing Records
    print("Deleting Existing Inventory Records on SQL Server")
    cursor.execute("TRUNCATE TABLE dbo.tblInventory")
    dbcnxn.commit()

    # Load spare part Inventory data onto SQL server
    print("Loading data to SQL server")
    strsql = """INSERT INTO dbo.tblInventory (PartNum,EngPartNum,Desc1,Desc2,Mfg,
                MfgPn,Cabinet,Drawer,OnHand,StandardCost,ReOrderDate,DeptUse,DeptPurch) 
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    try:
        cursor.executemany(strsql, dbase)
        dbcnxn.commit()
        print(str(len(dbase)) + " Records Processed")
    except Exception as e:
        msg = 'SQL Inventory Query Failed: ' + str(e)
        print(msg)
    return


def cleanup_pending():
    """Remove Entries from tblSparesPend that exist in tblInventory"""
    dbcnxn = pyodbc.connect(CONNSQL)
    cursor = dbcnxn.cursor()
    strsql = """DELETE FROM dbo.tblSparesPend 
                WHERE EXISTS (SELECT *
                              FROM dbo.tblInventory
                              WHERE dbo.tblInventory.EngPartNum = dbo.tblSparesPend.EngPn
                              )"""
    cursor.execute(strsql)
    dbcnxn.commit()
    return
