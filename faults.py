#!/usr/bin/env python

"""This Program moves fault logs to the SQL server.

When files are found they are parsed and their data is.
inserted into the database.
"""

import csv
import datetime
import os
import shutil
import pandas as pd
import pyodbc
import logging
import common_funcs
from datetime import date

# Setup Logging
# Gets or creates a logger
logger = logging.getLogger()

# set log level
logger.setLevel(logging.INFO)

FORMAT = '%Y%m%d%H%M%S'

# Define Database Connection

CONNECTION = """
Driver={SQL Server};
Server=tn-sql;
Database=autodata;
autocommit=true;
UID=production;
PWD=Auto@matics;
"""


def log_bad_row(badrow, machine):
    """Log rows that could not be processed."""
    badrowfilepath = "\\Inetpub\\ftproot\\MachFltBad\\"
    filenm = open(badrowfilepath + machine + "_" +
                  datetime.datetime.now().strftime(FORMAT) + ".log", "a")
    strdata = ','.join([str(z) for z in badrow])
    filenm.write(strdata)
    filenm.close()
    return


def get_faults():
    """Parse Fault Directories and Move Data to SQL  Server."""
    # connect
    dbcnxn = pyodbc.connect(CONNECTION)
    cursor = dbcnxn.cursor()

    # Set path to data directories
    dirpath = "\\Inetpub\\ftproot\\acmlogs\\"
    archive_path = "\\Inetpub\\ftproot\\fault_archive\\"
    today = date.today()

    # Get List of Data Files, Parse their Content and Insert into Database
    # str(datetime.timedelta(seconds=int(row[5]))),
    dirlist = os.listdir(dirpath)
    # Used to filter directory clutter
    ignore_list = ['Archive', 'logs', 'fltdata.csv', 'badfiles', 'FailedScrews', 'FailedLoks', 'temp', 'PassedLoks']
    for machine in dirlist:
        fltdatapath = dirpath + machine + "\\"
        filelist = os.listdir(fltdatapath)
        for filename in filelist:
            if (filename[len(filename) - 3:] == 'csv') and (filename not in ignore_list):
                print("Processing Fault Data for " + machine + ' File: ' + filename)
            else:
                continue

            if filename not in ignore_list:
                print("Processing: " + filename)
                inputfile = open(fltdatapath + filename)
                parser = csv.reader(inputfile)
                next(parser)
                badfile = 0
                for row in parser:
                    if not common_funcs.check_for_int(row[5]):
                        continue
                    if len(row) < 6 or int(row[5]) >= 86399:
                        continue
                    try:
                        # dtnm = datetime.datetime.strptime(row[0], '%m/%d/%Y')
                        dtnm = pd.to_datetime(row[0])
                    except pyodbc.Error:
                        log_bad_row(row, machine)
                        badfile = 1
                        msg = 'Date Format Problem: ' + fltdatapath + filename + ':' + str(row)
                        print(msg)
                        logger.error(msg + " [" + get_faults.__name__ + "]")
                        continue

                    if len(row) == 6:
                        sql = """
                            INSERT INTO 
                                 production.machflts (machine, timestamp, shift, operator, fault, duration)
                            VALUES (?, ?, ?, ?, ?, ?);
                        """
                        rowdata = (
                                   machine.upper(),
                                   row[0] + " " + row[1],
                                   row[2].upper(),
                                   row[3].rstrip(),
                                   row[4].rstrip(),
                                   row[5]
                                   )

                    if len(row) == 7:
                        sql = """
                            INSERT INTO 
                                 production.machflts (machine, timestamp, shift, operator, fault, duration, part)
                            VALUES (?, ?, ?, ?, ?, ?, ?);
                        """
                        rowdata = (
                                   machine.upper(),
                                   row[0] + " " + row[1],
                                   row[2].upper(),
                                   row[3].rstrip(),
                                   row[4].rstrip(),
                                   row[5],
                                   row[6]
                                   )

                    if dtnm.year >= today.year:
                        try:
                            cursor.execute(sql, rowdata)
                            dbcnxn.commit()
                        except pyodbc.Error as e:
                            log_bad_row(row, machine)
                            badfile = 1
                            msg = 'Could not add to database: ' + str(e) + ': ' \
                                  + fltdatapath + filename + ':' + str(row)
                            logger.error(msg + " [" + get_faults.__name__ + "]")
                            print(msg)
                            continue
                inputfile.close()
                if badfile == 1:
                    if not os.path.exists(fltdatapath + 'badfiles\\'):
                        os.makedirs(fltdatapath + 'badfiles\\')
                    shutil.move(fltdatapath + filename,
                                fltdatapath + 'badfiles\\' + filename)
                else:
                    # os.remove(fltdatapath + filename)
                    shutil.move(fltdatapath + filename, archive_path + machine + filename)
    dbcnxn.close()
    return
