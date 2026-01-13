#!/usr/bin/env python

"""Common Functions"""
import smtplib
from datetime import datetime, timedelta


# Define some Functions

def get_custom_week_number(date):
    """
    Calculate custom week number where week 1 is the first week with at least 4 days
    and weeks start on Sunday.

    Args:
        date: datetime object

    Returns:
        int: Week number for the given date
    """

    year = date.year
    jan_1 = datetime(year, 1, 1)

    # Find what day of week Jan 1 is (0=Monday, 6=Sunday)
    jan_1_weekday = jan_1.weekday()

    # Calculate days from Jan 1 to the next Sunday (start of first full week)
    # If Jan 1 is Sunday (6), days_to_sunday = 0
    # If Jan 1 is Monday (0), days_to_sunday = 6
    days_to_sunday = (6 - jan_1_weekday) % 7

    # First Sunday of the year
    first_sunday = jan_1 + timedelta(days=days_to_sunday)

    # Count how many days are in the partial week before first Sunday
    days_before_first_sunday = days_to_sunday

    # If there are 4+ days before the first Sunday (Thu, Fri, Sat, Sun = 4 days)
    # then Week 1 starts on the Sunday before or on Jan 1
    if days_before_first_sunday >= 4 or jan_1_weekday == 6:
        # Week 1 includes Jan 1
        week_1_start = jan_1 - timedelta(days=(jan_1_weekday + 1) % 7)
    else:
        # Week 1 starts on the first Sunday
        week_1_start = first_sunday

    # If date is before week 1 start, it belongs to last week of previous year
    if date < week_1_start:
        # Recursively get week number from previous year
        prev_year_end = datetime(year - 1, 12, 31)
        return get_custom_week_number(prev_year_end)

    # Calculate week number
    days_since_week_1 = (date - week_1_start).days
    week_number = (days_since_week_1 // 7) + 1

    return week_number


def check_for_int(data):
    """Test if argument can be an integer"""
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
