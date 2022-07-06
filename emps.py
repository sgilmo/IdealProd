#!/usr/bin/env python

"""Get employee information from AS400 and create new file emps.csv.

and put it in the ftproot on TN-DATACOLLECT
"""
import pyodbc
import csv

CONNECTION = """
Driver={iSeries Access ODBC Driver};
system=10.143.12.10;
Database=PROD;
UID=SMY;
PWD=SMY;
"""

emppath = "\\Inetpub\\ftproot\\"
englogin = ['9999', 'Elmer J Fudd', 'ENG']
nologin = ['0000', 'Please Log In', 'DEF']
eng = ['9999', '9107', '1656', '1472', '1626', '1351']

cnxn = pyodbc.connect(CONNECTION)
cursor = cnxn.cursor()

sql = """SELECT STRIP(EMP_CLOCK_NUMBER) As Clock,
                CONCAT(CONCAT(STRIP(EMP_FIRST_NAME), ' '),
                STRIP(EMP_LAST_NAME)) As Name,
                STRIP(EMP_POSITION_CODE) As Code
        FROM PROD.FPCLCKPAY
        WHERE (EMP_LOCATION = 09) AND (EMP_LAST_NAME <> 'TEMP')
        ORDER BY EMP_CLOCK_NUMBER"""

cursor.execute(sql)
file1 = [nologin, englogin]
for row in cursor:
    if row[0] in eng:
        row[2] = 'ENG'
    file1.append(row)
outputfile = open(emppath + "emps.csv", "w", newline='')
out = csv.writer(outputfile, delimiter=',', quoting=csv.QUOTE_NONE)
out.writerows(file1)
outputfile.close()
