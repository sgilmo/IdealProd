#!/usr/bin/env python

"""Functions Required for Engineering Spare Part Requisitions"""
import pyodbc
from sqlalchemy import text
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from sql_funcs import CONNECTION_STRING
from sql_funcs import engine


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
        start = """<html>
                <body>
                    <strong>Requested Spare Part(s):</strong><br><br />"""
        end = """       </body>
            </html>"""
        body = body + '<br><b>Sincerely,<br><br><br> The Engineering Overlords and Steve</b><br>'
        body = body + '<br><br><a href="https://www.idealtridon.com/idealtridongroup.html"> ' \
                        '<img src="https://sgilmo.com/email_logo.png" alt="Ideal Logo"></a>'
        # Attach the body content (HTML or plain text)
        message.attach(MIMEText(start+body+end, content_type))

        # Set up the SMTP connection
        with smtplib.SMTP(mail_server) as mail_server:
            mail_server.send_message(message)  # Send the email

        print(f"Email sent successfully to {to} with subject: {subject}")

    except Exception as e:
        print(f"Failed to send email: {e}")


def update_req():
    """Enter Timestamp for database records"""
    try:
        str_sql = """UPDATE dbo.tblReqSpare
                    SET dbo.tblReqSpare.reqdate = GETDATE()
                    WHERE dbo.tblReqSpare.reqdate IS NULL
                """
        with pyodbc.connect(CONNECTION_STRING) as dbcnxn:
            with dbcnxn.cursor() as cursor:
                cursor.fast_executemany = True
                cursor.execute(str_sql)
                dbcnxn.commit()
        print("Database updated successfully!")
        # Clean up resources
        dbcnxn.close()
    except pyodbc.Error as e:
        print(f"Database connection failed: {e}")


def make_req():
    """Make Request for New Spare Parts"""
    strsql = """
    SELECT * FROM dbo.tblReqSpare
    WHERE dbo.tblReqSpare.reqdate IS NULL
    """

    # Ensure SQL is a string and trimmed
    if not isinstance(strsql, str):
        raise TypeError("The SQL query must be a string.")
    strsql = strsql.strip()

    with engine.connect() as connection:
        df_reqspares = pd.read_sql_query(text(strsql), connection)
    if not df_reqspares.empty:
        print("Dataframe Size = ", df_reqspares.size)
        df_reqspares['cost'] = df_reqspares['cost'].round(2)
        df_reqspares['reqdate'] = datetime.now().date()
        # Add requestors to mailing list
        unique_reqby = df_reqspares['req_by'].drop_duplicates().tolist()
        mail_list = ["sgilmour@idealtridon.com", "bbrackman@idealtridon.com",
                     "rjobman@idealtridon.com", "mparman@idealtridon.com"]
        for item in unique_reqby:
            mail_list.append(item.lower() + "@idealtridon.com")
        print(mail_list)
        df_reqspares = df_reqspares[
            ['req_by', 'depts_using', 'desc', 'mfg', 'vendor', 'mfgpn', 'dwg', 'rev', 'cost', 'qty_to_stock',
             'qty_per_use', 'qty_annual_use', 'reorder_pt', 'reorder_amt']]
        # Renaming specific columns
        df_reqspares = df_reqspares.rename(columns={'req_by': 'Requested By', 'desc': 'Description',
                                                    'mfgpn': 'Manu Part Number', 'dwg': 'Drawing',
                                                    'rev': 'Revision', 'depts_using': 'Dept',
                                                    'mfg': 'Manufacturer', 'vendor': 'Vendor',
                                                    'cost': 'Cost', 'qty_to_stock': 'To Stock',
                                                    'qty_per_use': 'Per Use', 'qty_annual_use': 'Annual Usage',
                                                    'reorder_pt': 'Reorder Pt', 'reorder_amt': 'Reorder Amt'})

        main_content = df_reqspares.to_html(index=False)
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
        # mail_list = ["sgilmour@idealtridon.com"]
        send_email(mail_list, 'Please Add The Following Spare Parts', df_html)
        update_req()



