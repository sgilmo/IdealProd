#!/usr/bin/env python

"""Common Database Functions Used on the SQL Server"""

import pyodbc
import classes
import pandas as pd

# Define Database Connection


CONNSQL = """
Driver={SQL Server};
Server=tn-sql;
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
    strsql = """INSERT INTO dbo.tblUsage (Date,Part,EngPart,Dept,Acct,Clock,Machine,Qty,Cost,SubTotal) 
                VALUES (?,?,?,?,?,?,?,?,?,?)"""
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
                MfgPn,Cabinet,Drawer,OnHand,StandardCost,ReOrderDate,DeptUse,DeptPurch,ReOrderPt) 
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
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


def get_spare_req():
    """Get Spare Part Requests from SQL Server"""
    dbcnxn = pyodbc.connect(CONNSQL)
    cursor = dbcnxn.cursor()
    strsql = """SELECT * FROM dbo.tblReqSpare
                WHERE dbo.tblReqSpare.reqdate IS NULL
            """
    req_list = []
    for row in cursor.execute(strsql):
        req_spare = classes.Spare()
        req_spare.desc = row.desc
        req_spare.mfg_pn = row.mfgpn
        req_spare.dwg = row.dwg
        req_spare.mfg = row.mfg
        req_spare.vendor = row.vendor
        req_spare.unit = row.unit
        req_spare.cost = row.cost
        req_spare.qty_stock = row.qty_to_stock
        req_spare.qty_per_use = row.qty_per_use
        req_spare.depts = row.depts_using
        req_spare.qty_annual_use = row.qty_annual_use
        req_spare.req_by = row.req_by
        req_spare.reorder_pt = row.reorder_pt
        req_spare.reorder_amt = row.reorder_amt
        req_spare.eng_partnumber = row.eng_partnumber
        req_list.append(req_spare)
    return req_list


def update_req():
    """Enter Timestamp for database records"""
    dbcnxn = pyodbc.connect(CONNSQL)
    cursor = dbcnxn.cursor()
    strsql = """UPDATE dbo.tblReqSpare
                SET dbo.tblReqSpare.reqdate = GETDATE()
                WHERE dbo.tblReqSpare.reqdate IS NULL
            """
    cursor.execute(strsql)
    dbcnxn.commit()
    return


def move_entered_spares():
    """Move Entries from requested spare table that exist in tblInventory to
    the Entered Table (The point when the part was entered in the system)"""
    dbcnxn = pyodbc.connect(CONNSQL)
    cursor = dbcnxn.cursor()

    # Time stamp record when requested part is entered into the system (on the AS400).
    strsql = """UPDATE dbo.tblReqSpare                                        
                    SET dbo.tblReqSpare.reqdate = GETDATE()                    
                    WHERE EXISTS(SELECT *
                                  FROM dbo.tblInventory
                                  WHERE RTRIM(dbo.tblInventory.MfgPn) = RTRIM(dbo.tblReqSpare.mfgpn)
                                  )"""
    cursor.execute(strsql)
    dbcnxn.commit()

    # Insert Engineering Part Number into records of parts on the AS400
    strsql = """UPDATE dbo.tblReqSpare                                        
                        SET dbo.tblReqSpare.eng_partnumber = dbo.tblInventory.EngPartNum
                        FROM dbo.tblReqSpare
                        INNER JOIN dbo.tblInventory ON RTRIM(dbo.tblReqSpare.mfgpn) = RTRIM(dbo.tblInventory.MfgPn)                    
                        """
    cursor.execute(strsql)
    dbcnxn.commit()

    # Move records to entered table after purchasing was notified
    strsql = """INSERT INTO dbo.tblEnteredSpare
                SELECT *
                FROM dbo.tblReqSpare
                WHERE EXISTS(SELECT *
                              FROM dbo.tblInventory
                              WHERE RTRIM(dbo.tblInventory.MfgPn) = RTRIM(dbo.tblReqSpare.mfgpn)
                              )"""
    cursor.execute(strsql)
    dbcnxn.commit()

    # Remove records from Request Table if it has been entered into the AS400
    strsql = """DELETE FROM dbo.tblReqSpare 
                    WHERE EXISTS (SELECT *
                                  FROM dbo.tblInventory
                                  WHERE RTRIM(dbo.tblInventory.MfgPn) = RTRIM(dbo.tblReqSpare.mfgpn)
                                  )"""
    cursor.execute(strsql)
    dbcnxn.commit()
    return


def move_comp_spares():
    """Move Entries from Entered spare table that exist in tblInventory
    To Comp Table"""
    dbcnxn = pyodbc.connect(CONNSQL)
    cursor = dbcnxn.cursor()
    # Time stamp record to indicate the day the part was in stock
    strsql = """UPDATE dbo.tblEnteredSpare                    
                        SET dbo.tblEnteredSpare.reqdate = GETDATE() 
                        WHERE EXISTS(SELECT *
                                      FROM dbo.tblInventory
                                      WHERE dbo.tblInventory.MfgPn = dbo.tblEnteredSpare.mfgpn
                                      AND dbo.tblInventory.OnHand > 0
                                      )"""
    cursor.execute(strsql)
    dbcnxn.commit()

    # Move record to tblCompSpare. This is an archive of created spare parts
    strsql = """INSERT INTO dbo.tblCompSpare
                SELECT *
                FROM dbo.tblEnteredSpare
                WHERE EXISTS(SELECT *
                              FROM dbo.tblInventory
                              WHERE dbo.tblInventory.MfgPn = dbo.tblEnteredSpare.mfgpn
                              AND dbo.tblInventory.OnHand > 0
                              )"""
    cursor.execute(strsql)
    dbcnxn.commit()

    # Delete record in Entered Spare table if the part is in stock
    strsql = """DELETE FROM dbo.tblEnteredSpare 
                    WHERE EXISTS (SELECT *
                                  FROM dbo.tblInventory
                                  WHERE dbo.tblInventory.MfgPn = dbo.tblEnteredSpare.mfgpn
                                  AND dbo.tblInventory.OnHand > 0
                                  );"""
    cursor.execute(strsql)
    dbcnxn.commit()
    return


def find_new_obs():
    """Find any newly obsoleted spare parts"""
    dbcnxn = pyodbc.connect(CONNSQL)
    cursor = dbcnxn.cursor()
    new_obs = []
    strsql = """SELECT * FROM [dbo].[vwObsSpares]
                EXCEPT
                SELECT * FROM [dbo].[tblObsOrig]
                ;"""
    cursor.execute(strsql)
    for row in cursor:
        new_obs.append(row)
    return new_obs


def find_new_obs2(result_spares):
    """Find any newly obsoleted spare parts"""
    data_type_dict = {'StandardCost': float, 'OnHand': int, 'PartNum': str, 'ReOrderPt': int, 'ReOrderDate': int}
    df_spares = pd.DataFrame.from_records(result_spares)
    df_spares.columns = ['PartNum', 'EngPartNum', 'Desc1', 'Desc2', 'Mfg', 'MfgPn', 'Cabinet', 'Drawer', 'OnHand',
                         'StandardCost', 'ReOrderDate', 'DeptUse', 'DeptPurch', 'ReOrderPt']
    df_spares = df_spares.dropna()
    df_spares = df_spares.astype(data_type_dict)
    df_spares = df_spares.convert_dtypes()

    df_obs = df_spares[df_spares.Cabinet.str.contains('OBS')]
    df_obs_yest = pd.read_csv('c:\\temp\\yesterday_obs.csv', header=None, sep='\t')
    df_obs_yest.reset_index(drop=True, inplace=True)
    df_obs.reset_index(drop=True, inplace=True)
    df_obs_yest = df_obs_yest.fillna("")

    df_obs_yest.columns = df_spares.columns
    df_obs_yest = df_obs_yest.astype(data_type_dict)
    df_obs_yest = df_obs_yest.convert_dtypes()
    item_list = []
    if not df_obs['PartNum'].equals(df_obs_yest['PartNum']):
        df_diff = pd.concat([df_obs, df_obs_yest]).drop_duplicates(keep=False)
        df_obs.to_csv('c:\\temp\\yesterday_obs.csv', header=False, index=False, sep='\t')
        i = 1
        for index, row in df_diff.iterrows():
            # print(row['PartNum'], row['EngPartNum'], row['Desc1'])
            item_list.append('<h5>Item ' + str(i) + '</h5>'
                             + '<p style="margin-left: 40px">'
                             + 'Part Number: ' + row['PartNum']
                             + '<br>Eng Part Number: <strong>' + row['EngPartNum'] + '</strong>'
                             + '<br>Description 1: ' + row['Desc1']
                             + '<br>Description 2: ' + row['Desc2']
                             + '<br>Manufacturer: ' + row['Mfg']
                             + '<br>Manufacturer Pn: <strong>' + row['MfgPn'] + '</strong>'
                             + '<br>Dept Use: ' + row['DeptUse']
                             + '<br>Dept Purch: ' + row['DeptPurch']
                             + '</p><br>')
            i += 1
    return item_list

def insert_new_obs(new_obs):
    """Insert New Obsoleted Parts into tblObsOrig"""
    dbcnxn = pyodbc.connect(CONNSQL)
    cursor = dbcnxn.cursor()
    for item in new_obs:
        item_string = ', '.join('?' * len(item))
        strsql = """INSERT INTO [dbo].[tblObsOrig]
                    VALUES (%s);
                    """ % item_string
        cursor.execute(strsql, item)
        dbcnxn.commit()
    return


def save_new_obs(new_obs):
    """Insert New Obsoleted Parts into tblObsNew"""
    dbcnxn = pyodbc.connect(CONNSQL)
    cursor = dbcnxn.cursor()

    for item in new_obs:
        item_string = ', '.join('?' * len(item))
        strsql = """INSERT INTO [dbo].[tblObsNew]
                    VALUES (%s);
                    """ % item_string
        cursor.execute(strsql, item)
        dbcnxn.commit()
    return


def check_reorder_pts():
    """Find any changed reorder points"""
    dbcnxn = pyodbc.connect(CONNSQL)
    cursor = dbcnxn.cursor()
    changed = []

    strsql = """SELECT EngPartNum, Desc1, ReOrderPt FROM [dbo].[tblInventory]
                EXCEPT
                SELECT EngPartNum, Desc1, ReOrderPt FROM [dbo].[tblInvRef]
                ;"""
    cursor.execute(strsql)
    for row in cursor:
        changed.append(row)
    return changed


def log_changed_reorderpts(changed):
    """Insert Parts Changed Reorder Points into tblRePtChg"""
    dbcnxn = pyodbc.connect(CONNSQL)
    cursor = dbcnxn.cursor()
    for item in changed:
        item_string = ', '.join('?' * len(item))
        strsql = """INSERT INTO [dbo].[tblRePtChg]  (EngPartNum, Desc1, ReOrderPt)
                    VALUES (%s);
                    """ % item_string
        cursor.execute(strsql, item)
        dbcnxn.commit()
    return


def reset_ref_table():
    """Clear Out tblInvRef and Copy fresh values from tblInventory"""
    dbcnxn = pyodbc.connect(CONNSQL)
    cursor = dbcnxn.cursor()
    strsql = """DROP TABLE tblInvRef"""
    cursor.execute(strsql)
    dbcnxn.commit()
    strsql = """SELECT * INTO tblInvRef FROM tblInventory"""
    cursor.execute(strsql)
    dbcnxn.commit()
