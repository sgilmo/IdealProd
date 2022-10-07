#!/usr/bin/env python

"""Common Functions"""
import os
import smtplib
from twilio.rest import Client
import datetime


# Define some Functions


def check_for_int(data):
    """Test if argument can be an integer"""
    try:
        int(data)
        return True
    except ValueError:
        return False


def send_emaill(datalist, subject, message_header):
    """Send Email to Elab."""
    # Generate string list for email message
    str_msg = [message_header]
    for files in datalist:
        str_msg.append("     " + files + "\n")
    # Configure Email
    mail_server = "cas2013.ideal.us.com"
    sender = "datacollector@idealelab.com"
    mailto = ["mbarabas@idealtridon.com", "sgilmour@idealtridon.com"]
    text = ''.join(str_msg)
    message = """\
From: %s
To: %s
Subject: %s

%s
        """ % (sender, ", ".join(mailto), subject, text)
    # Send Email
    server = smtplib.SMTP(mail_server)
    server.sendmail(sender, mailto, message)
    server.quit()
    return


def send_text(msg):
    """Send SMS Message Using Twilio Account
    """
    account_sid = os.environ['TWILIO_ACCOUNT_SID']
    auth_token = os.environ['TWILIO_AUTH_TOKEN']
    client = Client(account_sid, auth_token)
    message = client.messages \
        .create(
            body=msg,
            from_='+19045670856',
            to='+19045011059'
        )
    print(message.sid)
    return


def scrub_data(row):
    """Clean up nonconforming entries"""
    # Make sure appropriate elements are floats and format them
    for x in range(4, 7):
        try:
            row[x] = '%.3f' % round(float(row[x]), 3)
        except ValueError:
            row[x] = 0.0
    # Sort out the bullshit entries
    row[7] = row[7].replace('Purchased ', '')
    row[7] = row[7].replace('\"', '')
    row[7] = row[7].replace("MX ONLY 10 mm ss", "10 mm")
    row[7] = row[7].replace("7 mm Umbrella", "7 mm")
    return row


def send_sparereq_email(datalist, subject, message_header):
    """Send Email to Purchasing Group."""
    # Generate string list for email message
    str_msg = [message_header]
    for files in datalist:
        str_msg.append("     " + files + "\n")
    sender = "elab@idealtridon.com"
    # mailto = ["sgilmour@idealtridon.com", "bbrackman@idealtridon.com", "kknight@idealtridon.com",
    # "rjobman@idealtridon.com"]
    # mailto = ["sgilmour@idealtridon.com", "bbrackman@idealtridon.com"] # For general beta testing
    mailto = ["sgilmour@idealtridon.com"]  # For debug/testing
    text = ''.join(str_msg)
    message = """\
From: %s
To: %s
MIME-Version: 1.0
Content-type: text/html
Subject: %s

%s
        """ % (sender, ", ".join(mailto), subject, text)
    # Send Email
    send_email(sender, mailto, message)
    return


def send_newobs_email(datalist, subject, message_header):
    """Send Email to Eng Group."""
    # Generate string list for email message
    str_msg = [message_header]
    for files in datalist:
        str_msg.append("     " + files + "\n")
    sender = "elab@idealtridon.com"
    mailto = ["sgilmour@idealtridon.com", "bbrackman@idealtridon.com"]
    # mailto = ["sgilmour@idealtridon.com"] # For Debug/Testing
    text = ''.join(str_msg)
    message = """\
From: %s
To: %s
MIME-Version: 1.0
Content-type: text/html
Subject: %s

%s
        """ % (sender, ", ".join(mailto), subject, text)
    # Send Email
    send_email(sender, mailto, message)
    return


def set_precision(num, prec):
    """Return Specific Precision of Floating Point Number"""
    return '{:.{}f}'.format(num, prec)


def send_email(sender, mailto, message):
    password = os.environ['TWILIO_ACCOUNT_SID']  # storing the password to log in
    mail_server = "mail.sgilmo.com"
    smtp_server = smtplib.SMTP(mail_server, 587)
    smtp_server.ehlo()  # setting the ESMTP protocol
    smtp_server.starttls()  # setting up to TLS connection
    smtp_server.ehlo()  # calling the ehlo() again as encryption happens on calling startttls()
    smtp_server.login(sender, password)  # logging into out email id
    # Send Email
    smtp_server.sendmail(sender, mailto, message)
    smtp_server.quit()
    return


def send_email_mach(datalist, subject, message_header):
    """Send Email to Eng Group, with list of data."""
    data_date = str((datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d'))
    sender = "datacollector@sgilmo.com"  # storing the sender's mail id
    mailto = ["elab@idealtridon.com", "steveg@sgilmo.com"]  # storing the receiver's mail id

    # Generate string list for email message
    str_msg = ["<h3>" + message_header + "</h3>"]
    for item in datalist:
        str_msg.append("     " + item + "<br>")
    str_msg.append("<br><br>"
                   + '<b>Sincerely,<br><br><br> The Engineering Overlords</b><br><i>Data Might be Missing For '
                   + data_date + '</i>')
    # Configure Email
    text = ''.join(str_msg)
    message = """\
MIME-Version: 1.0
Content-type: text/html
From: %s
To: %s
Subject: %s

%s
        """ % (sender, ", ".join(mailto), subject, text)
    # Send Email
    send_email(sender, mailto, message)
    return


def send_email_uptime(tbl_html, subject, message_header):
    """Send Email to Eng Group."""
    # storing the sender's mail id
    sender = "datacollector@sgilmo.com"
    # storing the receiver's mail id
    mailto = ["sgilmour@idealtridon.com", "steveg@sgilmo.com", "bbrackman@idealtridon.com",
              "jnapier@idealtridon.com", "jfinch@idealtridon.com", "thobbs@idealtridon.com"]

    # Generate string list for email message
    str_msg = ["<h3>" + message_header + "</h3>", "     " + tbl_html + "<br>",
               "<br><br>" + "<b>Sincerely,<br><br><br> The Engineering Overlords</b><br>"]
    # insert html table:
    # Configure Email
    text = ''.join(str_msg)
    message = """\
MIME-Version: 1.0
Content-type: text/html
From: %s
To: %s
Subject: %s

%s
        """ % (sender, ", ".join(mailto), subject, text)
    # Send Email
    send_email(sender, mailto, message)
    return


def send_email_progflt(flt_desc, subject, message_header):
    """Send Program Faults Email to Elab Group."""
    sender = "datacollector@sgilmo.com"  # storing the sender's mail id
    mailto = ["sgilmour@idealtridon.com", "steveg@sgilmo.com"]  # storing the receiver's mail id

    # Generate string list for email message
    str_msg = ["<h3>" + message_header + "</h3>", "     " + flt_desc + "<br>",
               "<br><br>" + "<b>Sincerely,<br><br><br> The Engineering Overlords</b><br>"]
    # insert html table:
    # Configure Email
    text = ''.join(str_msg)
    message = """\
MIME-Version: 1.0
Content-type: text/html
From: %s
To: %s
Subject: %s

%s
        """ % (sender, ", ".join(mailto), subject, text)
    # Send Email
    send_email(sender, mailto, message)
    return
