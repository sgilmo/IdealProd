#!/usr/bin/env python

"""This program finds available IP addresses in the assigned subnets 10.143.50.xxx and 10.143.51.xxx"""

import socket
import smtplib
import getpass
import pyodbc
import os

# Constants
SUBNETS = ['10.143.50.', '10.143.51.']
SQL_CONNECTION_STRING = f"""
Driver={{SQL Server}};
Server=tn-sql;
Database=autodata;
autocommit=true;
UID={os.getenv('SQL_UID')};
PWD={os.getenv('SQL_PWD')};
"""
MAIL_SERVER = "cas2013.ideal.us.com"
SENDER_EMAIL = "towerofdespair@idealelab.com"


def get_all_ips():
    """Build list of all possible IP addresses in subnets."""
    ip_addresses = []
    for subnet in SUBNETS:
        if subnet == '10.143.50.':
            ip_addresses.extend([subnet + str(x) for x in range(1, 249)])
        elif subnet == '10.143.51.':
            ip_addresses.extend([subnet + str(x) for x in range(1, 253)])
    return ip_addresses


def get_used_ips():
    """Build list of used IP addresses from the database."""
    used_ips = []
    query_template = """SELECT IP FROM dbo.tblIP WHERE LEFT(IP, 9) = '{}' ORDER BY IP"""
    cnxn = pyodbc.connect(SQL_CONNECTION_STRING)
    cursor = cnxn.cursor()

    for subnet in SUBNETS:
        cursor.execute(query_template.format(subnet.rstrip('.')))
        used_ips.extend(row[0] for row in cursor)

    return used_ips


def get_avail_ips():
    """Build list of available IP addresses."""
    all_ips = set(get_all_ips())
    used_ips = set(get_used_ips())
    available_ips = all_ips - used_ips
    return sorted(available_ips, key=socket.inet_aton)


def load_database(data):
    """Insert new IP addresses into database."""
    cnxn = pyodbc.connect(SQL_CONNECTION_STRING)
    cursor = cnxn.cursor()

    for record in data:
        sql = "INSERT INTO dbo.tblIP (Mach, Location, Type, IP) VALUES (?, ?, ?, ?)"
        cursor.execute(sql, record)
        cnxn.commit()


def send_email(data, subject, header):
    """Send email to user with reserved IP addresses."""
    user = getpass.getuser()
    email_body = f"{header}\n\n{data[0][0]}: \n\n"
    email_body += "\n".join([f"                 {rec[2]} : {rec[3]}" for rec in data])

    message = f"From: {SENDER_EMAIL}\nTo: {user}@idealtridon.com\nSubject: {subject}\n{email_body}\n"

    server = smtplib.SMTP(MAIL_SERVER)
    server.sendmail(SENDER_EMAIL, [f"{user}@idealtridon.com"], message)
    server.quit()


def main():
    """Main function."""
    user = getpass.getuser()
    available_ips = get_avail_ips()

    while True:
        try:
            num_ip_required = int(input("How Many IP Addresses Will be Required: "))
            break
        except ValueError:
            print('Must be a number, try again')

    while True:
        equipment_id = input("Equipment ID: ")
        if len(equipment_id) >= 3:
            break

    equipment_id = equipment_id.upper()

    subsystem_types = []
    for _ in range(num_ip_required):
        while True:
            subsystem_type = input("Subsystem Type (PLC, HMI, SERVO, CPU, CAM, etc...): ")
            if len(subsystem_type) >= 3:
                subsystem_types.append(subsystem_type.upper())
                break

    data = [[equipment_id, 'SMYRNA', subsystem_type, available_ips[idx]] for idx, subsystem_type in
            enumerate(subsystem_types)]

    print("\n\n")
    for record in data:
        print(f"For {record[2]} Use {record[3]}\n")

    send_email(data, 'IP Addresses Reserved', f'You ({user}) have reserved the following IP addresses for ')

    load_database(data)

    print(f"\n\nIP Addresses Remaining in Subnet: {len(available_ips) - num_ip_required}\n\n\n")
    input("Press any key to exit")


if __name__ == '__main__':
    main()