#!/usr/bin/env python

"""Common Functions"""

import smtplib

# Define some Functions


def check_for_int(data):
    """Test if argument can be an integer"""
    try:
        int(data)
        return True
    except ValueError:
        return False


def send_email(datalist, subject, message_header):
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


def send_text(strfrm, msgto, strmsg):
    """Establish a secure session with gmail's outgoing SMTP server using my gmail account# Use sms gateway provided
     by mobile carrier:
                at&t:     number@mms.att.net
                t-mobile: number@tmomail.net
                verizon:  number@vtext.com
                sprint:   number@page.nextel.com

    :param strfrm: From
    :param msgto: List of Addresees
    :param strmsg: The Message to be texted
    """

    directory = {'mbarab':  '9314099955@vtext.com', 'sgilmo':  '9045011059@mms.att.net'}
    tolist = []
    for item in msgto:
        tolist.append(directory[item])
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()

    server.login('sgilmout@gmail.com', 'XqMJyAaZ3k*q')
    message = """\
From:  %s
To:  %s
Subject: %s

    %s
            """ % (strfrm, ",  ".join(tolist), '', strmsg)
    server.sendmail(strfrm, tolist, message)
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
