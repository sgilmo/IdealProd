#!/usr/bin/env python


"""This program finds available IP addresses.

the Elab assigned subnet 10.143.50.xxx
and 10.143.51.xxx
"""

import socket
import smtplib
import getpass
import pyodbc

# Define Database Connection

# Old Driver
# Driver={SQL Server Native Client 11.0};
CONNECTION = """
Driver={SQL Server};
Server=tn-sql;
Database=autodata;
autocommit=true;
UID=production;
PWD=Auto@matics;
"""


def get_all_ips():
    """Build list of all possible IP addresses in subnet."""
    # initialize list for all IP's in subnet
    iplist = []
    subnet = '10.143.50.'
    for x in range(1, 249):
        iplist.append(subnet + str(x))
    subnet = '10.143.51.'
    for x in range(1, 253):
        iplist.append(subnet + str(x))
    return iplist


def get_used_ips():
    """Build list of used IP addresses."""
    # initialize list for all used IP's in subnet
    iplist = []

    # connect to the SQL Server
    cnxn = pyodbc.connect(CONNECTION)
    cursor = cnxn.cursor()
    sql = """SELECT IP
          FROM dbo.tblIP
          WHERE (LEFT(IP,9) = '10.143.50')
          ORDER BY IP
          """
    cursor.execute(sql)
    for row in cursor:
        iplist.append(row[0])
    sql = """SELECT IP
          FROM dbo.tblIP
          WHERE (LEFT(IP,9) = '10.143.51')
          ORDER BY IP
          """
    cursor.execute(sql)
    for row in cursor:
        iplist.append(row[0])
    return iplist


def get_avail_ips():
    """Build list of available IP addresses."""
    # Get list of differences
    iplist = set(get_all_ips()) - set(get_used_ips())
    # Create list of sorted IPs
    tempips = [socket.inet_aton(ip) for ip in iplist]
    tempips.sort()
    iplist = [socket.inet_ntoa(ip) for ip in tempips]
    return iplist


def load_database(data):
    """Insert new IPs into database."""
    # Get number of elements
    sz = len(data)

    # Connect to SQL server
    cnxn = pyodbc.connect(CONNECTION)
    cursor = cnxn.cursor()

    # Insert new data into database
    for x in range(0, sz):
        sql = "INSERT INTO dbo.tblIP (Mach,Location,Type,IP) " \
              "VALUES ('%s','%s','%s','%s')" % \
              (data[x][0], data[x][1], data[x][2], data[x][3])
        cursor.execute(sql)
        cnxn.commit()
    return


def send_email(data, subject, msghdr):
    """Send email to user."""
    # Get current user
    user = getpass.getuser()
    print(user)
    # Get number of elements
    sz = len(data)

    # Generate string list for email message
    str_msg = [msghdr, data[0][0] + ': \n\n']
    for x in range(0, sz):
        str_msg.append("                 " + data[x][2] +
                       ' : ' + data[x][3] + "\n")

    # Configure Email
    # old mailserver = "10.143.12.57"
    mailserver = "mail.idealtridon.com"
    sender = "towerofdespair@idealelab.com"
    to = [user + "@idealtridon.com"]
    text = ''.join(str_msg)
    message = """\
From: %s
To: %s
Subject: %s

%s
        """ % (sender, ", ".join(to), subject, text)

    # Send Email
    server = smtplib.SMTP(mailserver)
    server.sendmail(sender, to, message)
    server.quit()
    return


def main():
    """Main function."""
    casetype = []
    iplist = get_avail_ips()
    data = []

    # Get current user
    user = getpass.getuser()

    # Get number of elements
    sz = len(iplist)

    # Get Information from user
    while True:
        try:
            numip = int(input("How Many IP Addresses Will be Required: "))
            break
        except ValueError:
            print('Must be a number, try again')
    while True:
        equip = input("Equipment ID: ")
        if len(equip) >= 3:
            break

    # Convert equip to upper case
    equip = equip.upper()

    # Get type info from user
    for x in range(0, numip):
        while True:
            ans = input("Subsystem Type for IP# " + str(x + 1) +
                        '(PLC, HMI, SERVO, CPU, CAM, etc...): ')
            if len(ans) >= 3:
                break
        casetype.append(ans)

    # Convert type to upper case
    casetype = [x.upper() for x in casetype]

    # Display results
    print('\n\n')
    for x in range(0, numip):
        print(('For ' + casetype[x] + ' Use ' + iplist[x] + '\n'))
        data.append([equip, 'SMYRNA', casetype[x], iplist[x]])

    # Send email to user
    send_email(data, 'IP Addresses Reserved',
               'You (' + user + ') have reserved the following IP addresses for ')

    # Enter assigned IPs into database
    load_database(data)

    # Display number of remaining IPs in pool
    rem = str(sz - numip)
    print(('\n\nIP Addresses Remaining in Subnet: ' + rem))

    # Wait for operator before exiting
    print('\n\n\n\n')
    input("Press any key to exit")
    return


if __name__ == '__main__':
    main()
