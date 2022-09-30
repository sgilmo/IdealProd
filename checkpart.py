# coding=utf-8
"""Test Program to sort out Barry Nixon"""
# !/usr/bin/env python

import pyodbc


def get_filemaker_items():
    """Get Current part data file from Filemaker database."""
    cnxn = pyodbc.connect('DSN=FM Clamp ODBC;UID=FMODBC;PWD=FMODBC')
    cursor = cnxn.cursor()
    print('Connected to FileMaker Server\n\n')
    input1 = input('Enter Part Number: ')
    input1 = "'" + input1 + "'"
    print('\n')
    sql = """
            SELECT Ourpart,"Band A Part Number", "Housing A Part Number",
                "Screw Part Number" AS Screw, "Band Feed from Band data", "Ship Diam Max",
                "Ship Diam Max QA Alternate", "Ship Diam Min", "Hex Size", "Band_Thickness",
                "Band_Width", "CameraInspectionRequired", "ScrDrvChk"
            FROM tbl8Tridon 
            WHERE  (Ourpart Like %s)
            ORDER BY Ourpart
        """ % input1
    cursor.execute(sql)
    i = 0
    row = ''
    for row in cursor:
        i += 1
        # f.write(row[0] + '\n')
        print('Part: ' + repr(str(row[0])))
        print('Band: ' + repr(str(row[1])))
        print('Housing: ' + repr(str(row[2])))
        print('Screw: ' + repr(str(row[3])))
        print('Bandfeed: ' + repr(str(row[4])))
        print('Ship Dia Max: ' + repr(str(row[5])))
        print('Ship Dia Max (Alt): ' + repr(str(row[6])))
        print('Ship Dia Min: ' + repr(str(row[7])))
        print('Hex Size: ' + repr(str(row[8])))
        print('Band Thickness: ' + repr(str(row[9])))
        print('Band Width: ' + repr(str(row[10])))
        print('Camera Inspect: ' + repr(str(row[11])))
        print('Screwdriver Check: ' + repr(str(row[12])))
    cnxn.close()
    if len(row) == 0:
        print("No Part Found")
    return


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
