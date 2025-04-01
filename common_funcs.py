#!/usr/bin/env python

"""Common Functions"""
import smtplib


# Define some Functions

def check_for_int(data):
    """Test if argument can be an integer"""
    print(data)
    try:
        int(data)
        return True
    except (NameError, ValueError, Exception):
        return False


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


def set_precision(num, prec):
    """Return Specific Precision of Floating Point Number"""
    return '{:.{}f}'.format(num, prec)


def send_email(sender, mailto, message):
    # Send Email
    mail_server = "cas2013.ideal.us.com"
    smtp_server = smtplib.SMTP(mail_server)
    message = message + '<br><b>Sincerely,<br><br><br> The Engineering Overlords and Steve</b><br>'
    message = message + '<br><br><a href="https://www.idealtridon.com/idealtridongroup.html"> ' \
                        '<img src="https://sgilmo.com/email_logo.png" alt="Ideal Logo"></a>'
    smtp_server.sendmail(sender, mailto, message.replace('\xb0', ''))
    smtp_server.quit()
    return


def build_email(body_data, subject, message_header, mailto):
    """Send Program Faults Email to Elab Group."""
    sender = "elab@idealtridon.com"  # storing the sender's mail id
    # Generate string list for email message
    str_msg = ["<h3>" + message_header + "</h3>", "     " + "<br>".join(body_data) + "<br>"]
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
    send_email(sender, mailto, message)
    return


def build_email2(body_data, subject, message_header, mailto):
    """Send Program Faults Email to Elab Group."""
    sender = "elab@idealtridon.com"  # storing the sender's mail id
    # Generate string list for email message
    str_msg = ["<h3>" + message_header + "</h3>", "     " + body_data + "<br>"]
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
    send_email(sender, mailto, message)
    return
