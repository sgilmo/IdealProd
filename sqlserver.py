#!/usr/bin/env python

"""Common Database Functions Used on the SQL Server"""

import pyodbc
import classes
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy.exc
from urllib import parse
import as400
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Define Database Connection


CONNSQL = """
Driver={SQL Server};
Server=tn-sql;
Database=autodata;
UID=production;
PWD=Auto@matics;
"""

server = 'tn-sql'
database = 'autodata'
driver = 'ODBC+Driver+17+for+SQL+Server&AUTOCOMMIT=TRUE'
user = 'production'
pwd = parse.quote_plus("Auto@matics")
port = '1433'
database_conn = f'mssql+pyodbc://{user}:{pwd}@{server}:{port}/{database}?driver={driver}'
# Make Connection
engine = create_engine(database_conn)
conn = engine.connect()


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

def find_new_obs(result_spares):
    """Find any newly obsoleted spare parts"""
    data_type_dict = {'StandardCost': float, 'OnHand': int, 'PartNum': str, 'ReOrderPt': int,
                      'ReOrderDate': int, 'Cabinet': str, 'Drawer': str}
    # Ensure the correct structure of incoming data
    try:
        df_spares = pd.DataFrame.from_records(result_spares)
        expected_columns = ['PartNum', 'EngPartNum', 'Desc1', 'Desc2', 'Mfg', 'MfgPn',
                            'Cabinet', 'Drawer', 'OnHand', 'StandardCost', 'ReOrderDate',
                            'DeptUse', 'DeptPurch', 'ReOrderPt']
        if len(df_spares.columns) != len(expected_columns):
            raise ValueError("Mismatch in the number of columns in result_spares")
        df_spares.columns = expected_columns
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of failure

    # Drop NaN values from critical columns
    df_spares = df_spares.dropna(subset=['PartNum', 'Cabinet', 'OnHand'])

    # Enforce data types safely
    for col, dtype in data_type_dict.items():
        if col in df_spares.columns:
            if dtype == int or dtype == float:
                df_spares[col] = pd.to_numeric(df_spares[col], errors='coerce')
            else:
                df_spares[col] = df_spares[col].astype(dtype)

    # Filter for obsolete parts
    df_obs_all = df_spares[df_spares.Cabinet.str.contains('OBS', case=False, na=False)]

    try:
        df_obs_current = pd.read_sql("SELECT PartNum FROM eng.tblObsSpares", engine)
    except Exception as e:
        print(f"Database query error: {e}")
        return pd.DataFrame()  # Return an empty DataFrame

    # Identify new obsolete parts
    df_obs_new = df_obs_all[~df_obs_all['PartNum'].isin(df_obs_current['PartNum'])]
    return df_obs_new

def add_obs(df):
    """Add new Obs Spare Parts to the Obs Spare Parts Table"""
    table_name = 'tblObsSpares'
    df.to_sql(
        name=table_name,
        con=engine,
        schema='eng',
        if_exists='append',
        index=False,
        dtype= {
                "PartNum": sqlalchemy.types.VARCHAR(length=255),
             "EngPartNum": sqlalchemy.types.VARCHAR(length=255),
        }
    )
    return

def check_obs(inv):
    df_obs_spares = find_new_obs(inv)
    df_obs_nums = df_obs_spares[['PartNum', 'EngPartNum']]

    # Renaming specific columns
    df_obs_spares = df_obs_spares.rename(columns={'PartNum': 'Part Number', 'EngPartNum': 'Eng Part Number',
                                                  'Desc1': 'Description 1', 'Desc2': 'Description 2'})

    mail_list = ['sgilmour@idealtridon.com', 'bbrackman@idealtridon.com']

    if df_obs_spares.size > 0:
        df_html_table = df_obs_spares.iloc[:, :4].to_html(index=False, border=1)
        send_email(mail_list, 'The Following Items Were Just Made Obsolete', df_html_table)
        add_obs(df_obs_nums)
    else:
        print("No New Obsolete Parts")
    return

def send_email(to, subject, body, content_type='html', username='elab@idealtridon.com'):
    # Send Email
    mail_server = "cas2013.ideal.us.com"

    if isinstance(to, list):
        # Join the list of email addresses into a single string
        to = ', '.join(to)

    try:
    # Create a MIME email
        message = MIMEMultipart()
        message['From'] = username
        message['To'] = to
        message['Subject'] = subject
        body = body + '<br><b>Sincerely,<br><br><br> The Engineering Overlords and Steve</b><br>'
        body = body + '<br><br><a href="https://www.idealtridon.com/idealtridongroup.html"> ' \
                        '<img src="https://sgilmo.com/email_logo.png" alt="Ideal Logo"></a>'
        # Attach the body content (HTML or plain text)
        message.attach(MIMEText(body, content_type))
        # Set up the SMTP connection
        with smtplib.SMTP(mail_server) as mserver:
            mserver.send_message(message)  # Send the email

        print(f"Email sent successfully to {to} with subject: {subject}")

    except Exception as e:
        print(f"Failed to send email: {e}")


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
