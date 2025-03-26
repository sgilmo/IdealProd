#!/usr/bin/env python

"""Common Database Functions Used on the SQL Server"""

import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text
import sqlalchemy.exc
from urllib import parse
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Define constants
DATA_TYPE_DICT = {
    'StandardCost': float,
    'OnHand': int,
    'PartNum': str,
    'ReOrderPt': int,
    'ReOrderDate': int,
    'Cabinet': str,
    'Drawer': str
}
EXPECTED_COLUMNS = [
    'PartNum', 'EngPartNum', 'Desc1', 'Desc2', 'Mfg', 'MfgPn',
    'Cabinet', 'Drawer', 'OnHand', 'StandardCost', 'ReOrderDate',
    'DeptUse', 'DeptPurch', 'ReOrderPt'
]

# Define Database Connections

CONNSQL = """
Driver={SQL Server};
Server=tn-sql;
Database=autodata;
UID=production;
PWD=Auto@matics;
"""

server = 'tn-sql'
database = 'autodata'
driver = 'ODBC+Driver+17+for+SQL+Server'
user = 'production'
pwd = parse.quote_plus("Auto@matics")
port = '1433'
database_conn = f'mssql+pyodbc://{user}:{pwd}@{server}:{port}/{database}?driver={driver}'
# Make Connection
engine = create_engine(database_conn)


def update_dbusage(dbase):
    """ Add Spare Part Usage Data to SQL server database"""
    print("Deleting Existing Records on SQL Server")
    with pyodbc.connect(CONNSQL) as dbcnxn:
        with dbcnxn.cursor() as cursor:
            cursor.fast_executemany = True
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
        dbcnxn.close()
    except Exception as e:
        msg = 'SQL Query Failed: ' + str(e)
        print(msg)
    return


def update_emps(dbase):
    """Add Employee data to SQL server database"""
    # Delete Existing Records
    print("Deleting Existing Employee Records on SQL Server")
    with pyodbc.connect(CONNSQL) as dbcnxn:
        with dbcnxn.cursor() as cursor:
            cursor.fast_executemany = True
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
        dbcnxn.close()
    except Exception as e:
        msg = 'SQL Inventory Query Failed: ' + str(e)
        print(msg)
    return


def update_dbinv(dbase):
    """ Add Spare Part Inventory Data to SQL server database"""
    print("Deleting Existing Inventory Records on SQL Server")
    with pyodbc.connect(CONNSQL) as dbcnxn:
        with dbcnxn.cursor() as cursor:
            cursor.fast_executemany = True
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
        dbcnxn.close()
    except Exception as e:
        msg = 'SQL Inventory Query Failed: ' + str(e)
        print(msg)
    return


def cleanup_pending():
    """Remove Entries from tblSparesPend that exist in tblInventory"""
    strsql = """DELETE FROM dbo.tblSparesPend 
                WHERE EXISTS (SELECT *
                              FROM dbo.tblInventory
                              WHERE dbo.tblInventory.EngPartNum = dbo.tblSparesPend.EngPn
                              )"""
    with pyodbc.connect(CONNSQL) as dbcnxn:
        with dbcnxn.cursor() as cursor:
            cursor.fast_executemany = True
            cursor.execute(strsql)
            dbcnxn.commit()
    dbcnxn.close()
    return


def move_entered_spares():
    """Move Entries from requested spare table that exist in tblInventory to
    the Entered Table (The point when the part was entered in the system)"""
    # dbcnxn = pyodbc.connect(CONNSQL)
    # cursor = dbcnxn.cursor()

    # Time stamp record when requested part is entered into the system (on the AS400).
    strsql = """UPDATE dbo.tblReqSpare                                        
                    SET dbo.tblReqSpare.reqdate = GETDATE()                    
                    WHERE EXISTS(SELECT *
                                  FROM dbo.tblInventory
                                  WHERE RTRIM(dbo.tblInventory.MfgPn) = RTRIM(dbo.tblReqSpare.mfgpn)
                                  )"""
    with pyodbc.connect(CONNSQL) as dbcnxn:
        with dbcnxn.cursor() as cursor:
            cursor.fast_executemany = True
            cursor.execute(strsql)
            dbcnxn.commit()
    # cursor.execute(strsql)
    # dbcnxn.commit()

    # Insert Engineering Part Number into records of parts on the AS400
    strsql = """UPDATE dbo.tblReqSpare                                        
                        SET dbo.tblReqSpare.eng_partnumber = dbo.tblInventory.EngPartNum
                        FROM dbo.tblReqSpare
                        INNER JOIN dbo.tblInventory ON RTRIM(dbo.tblReqSpare.mfgpn) = RTRIM(dbo.tblInventory.MfgPn)                    
                        """
    with pyodbc.connect(CONNSQL) as dbcnxn:
        with dbcnxn.cursor() as cursor:
            cursor.fast_executemany = True
            cursor.execute(strsql)
            dbcnxn.commit()
    # cursor.execute(strsql)
    # dbcnxn.commit()

    # Move records to entered table after purchasing was notified
    strsql = """INSERT INTO dbo.tblEnteredSpare
                SELECT *
                FROM dbo.tblReqSpare
                WHERE EXISTS(SELECT *
                              FROM dbo.tblInventory
                              WHERE RTRIM(dbo.tblInventory.MfgPn) = RTRIM(dbo.tblReqSpare.mfgpn)
                              )"""
    with pyodbc.connect(CONNSQL) as dbcnxn:
        with dbcnxn.cursor() as cursor:
            cursor.fast_executemany = True
            cursor.execute(strsql)
            dbcnxn.commit()
    # cursor.execute(strsql)
    # dbcnxn.commit()

    # Remove records from Request Table if it has been entered into the AS400
    strsql = """DELETE FROM dbo.tblReqSpare 
                    WHERE EXISTS (SELECT *
                                  FROM dbo.tblInventory
                                  WHERE RTRIM(dbo.tblInventory.MfgPn) = RTRIM(dbo.tblReqSpare.mfgpn)
                                  )"""
    with pyodbc.connect(CONNSQL) as dbcnxn:
        with dbcnxn.cursor() as cursor:
            cursor.fast_executemany = True
            cursor.execute(strsql)
            dbcnxn.commit()
    dbcnxn.close()
    # cursor.execute(strsql)
    # dbcnxn.commit()
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


def create_dataframe_safe(result_spares):
    """Safely create and validate a DataFrame from result_spares."""
    try:
        df = pd.DataFrame.from_records(result_spares)
        if len(df.columns) != len(EXPECTED_COLUMNS):
            raise ValueError("Mismatch in the number of columns in result_spares")
        df.columns = EXPECTED_COLUMNS
        return df
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        return pd.DataFrame()


def enforce_data_types(df):
    """Ensure that columns have the correct data types."""
    for col, dtype in DATA_TYPE_DICT.items():
        if col in df.columns:
            if dtype in {int, float}:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                df[col] = df[col].astype(dtype)


def filter_obsolete_parts(df):
    """Filter the DataFrame for parts marked as obsolete."""
    return df[df['Cabinet'].str.contains('OBS', case=False, na=False)]


def fetch_current_obsolete_parts():
    """Fetch the current list of obsolete parts from the database."""
    try:
        strsql = "SELECT PartNum FROM eng.tblObsSpares"
        return pd.DataFrame(engine.connect().execute(text(strsql)))
    except Exception as e:
        print(f"Database query error: {e}")
        return pd.DataFrame()


def find_new_obs(result_spares):
    """Find any newly obsoleted spare parts."""
    df_spares = create_dataframe_safe(result_spares)
    if df_spares.empty:
        return pd.DataFrame()  # Exit early if DataFrame creation failed

    # Drop NaN values from critical columns and enforce data types
    df_spares = df_spares.dropna(subset=['PartNum', 'Cabinet', 'OnHand'])
    enforce_data_types(df_spares)

    # Filter obsolete parts
    df_all_obsolete_parts = filter_obsolete_parts(df_spares)

    # Fetch currently known obsolete parts
    df_current_obsolete_parts = fetch_current_obsolete_parts()
    if df_current_obsolete_parts.empty:
        return pd.DataFrame()  # Exit early if database query failed

    # Identify new obsolete parts
    df_new_obsolete_parts = df_all_obsolete_parts[
        ~df_all_obsolete_parts['PartNum'].isin(df_current_obsolete_parts['PartNum'])
    ]

    return df_new_obsolete_parts

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
    # Obtain obsolete spares DataFrame
    df_obs_spares = find_new_obs(inv)

    # Debugging: Print the columns of the dataframe to verify structure
    # print("Columns in df_obs_spares:", df_obs_spares.columns.tolist())

    # Check if required columns exist
    required_columns = ['PartNum', 'EngPartNum']
    if all(column in df_obs_spares.columns for column in required_columns):
        # If columns exist, extract the required subset
        df_obs_nums = df_obs_spares[required_columns]
    else:
        # Handle the case where columns are missing
        print(f"Error: One or more required columns {required_columns} are missing from the input data.")
        print("No action taken on obsolete spares.")
        return

    # Proceed with renaming columns and sending emails
    df_obs_spares = df_obs_spares.rename(columns={'PartNum': 'Part Number', 'EngPartNum': 'Eng Part Number',
                                                  'Desc1': 'Description 1', 'Desc2': 'Description 2'})

    mail_list = ['sgilmour@idealtridon.com', 'bbrackman@idealtridon.com']
    # mail_list = ['sgilmour@idealtridon.com']

    # Check if there are any obsolete spares
    if not df_obs_spares.empty:
        # Create HTML table for email
        main_content = df_obs_spares.iloc[:, :4].to_html(index=False, border=1)
        df_html = f"""
                    <html>
                    <head>           
                    <style>
                        thead {{color: black;}}
                        tbody {{color: black; }}
                        tfoot {{color: red;}}
                        table, th, td {{
                            border: 0px solid black;
                            padding: 5px;
                        }}
                    </style>                
                    </head>
                    <body>
                        {main_content}
                    </body>
                    </html>
                """
        # Sending email and adding to obsolete spares database
        send_email(mail_list, 'The Following Items Were Just Made Obsolete', df_html)
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
