# coding=utf-8
"""Test Program to sort out Barry Nixon"""
# !/usr/bin/env python

import pyodbc
import common_funcs
import os


def get_filemaker_items():
    """Get Current part data file from Filemaker database."""
    print('Connecting to FileMaker Server, it will take a bit\n\n')
    cnxn = pyodbc.connect('DSN=FM Clamp ODBC;UID=FMODBC;PWD=FMODBC')
    cursor = cnxn.cursor()
    os.system('cls')
    print('Connected to FileMaker Server\n\n')
    input1 = input('Enter Part Number: ')
    input1 = "'" + input1 + "'"
    print('\n')
    os.system('cls')
    sql = """
            SELECT Ourpart,"Band A Part Number", "Housing A Part Number",
                "Screw Part Number" AS Screw, "Band Feed from Band data",
                "Ship Diam Max", "Ship Diam Min", "Hex Size", "Band_Thickness", "Band_Width",
                "CameraInspectionRequired", "ScrDrvChk"
            FROM tbl8Tridon 
            WHERE  (Ourpart = %s)
            ORDER BY Ourpart
        """ % input1
    cursor.execute(sql)
    i = 0
    row = ''
    for row in cursor:
        i += 1
        # f.write(row[0] + '\n')
        print('Part: ' + str(row[0]) + '\n')
        print('Band: ' + str(row[1]), 'Bandfeed: ' + str(row[4]), 'Housing: ' + str(row[2]),
              'Screw: ' + str(row[3]), 'Hex Size: ' + str(row[7]))
        print('Ship Dia Min: ' + str(row[6]), 'Ship Dia Max: ' + str(row[5]))
        print('Band Thickness: ' + str(row[8]), 'Band Width: ' + str(row[9]))
        print('Camera: ' + str(row[10]), 'Screwdriver Check: ' + str(row[11]))
    cnxn.close()
    if len(row) == 0:
        print("No Part Found")
    return


def cleandata(row):
    """ Cleanup bad data in database row"""
    if row[5] == "":
        row[5] = 0.00
    if row[6] == "":
        row[6] = 0.00
    # Get rid of bad part number entry
    row[0] = row[0].replace('\xa0', '')
    # Get rid of leading and following spaces
    for x in range(11):
        try:
            row[x] = str(row[x]).strip()
        except Exception as e:
            msg = 'Machine Update Failed: ' + str(e)
            print(msg)
    return common_funcs.scrub_data(row)


def main():
    """

    :return:
    """
    get_filemaker_items()
    # Wait for operator before exiting
    print('\n')
    input("Press any key to exit\n")
    return


if __name__ == '__main__':
    main()
