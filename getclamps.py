# coding=utf-8
# !/usr/bin/env python
"""This Program update local machine part databases.

Compares the current machine part data
file with the active product database if there is a difference, the current
parameter file (parts.csv) is replaced with a new one a copy of the previous
file will be made and moved to the parthistory folder with a datestamp for
a name. A log file will also be generated with a list of partnumbers that
were added, modified or deleted. The file (parts.csv) will then be ftp'd
to every machine that uses this data. Any machines that could not be updated
will be added to the 'badlist.csv' file and the elab will be notified via
email. The program also monitors the 'MAINT' directory for updated floor
documentation submitted by the Maint personel. Any files found will be moved
to the 'ReviewQue' directory and the Elab will be notified via email.
"""

import csv
import ftplib
import os
import shutil
from datetime import datetime
from subprocess import Popen, PIPE
import pyodbc
import CommonFunc
from timeit import default_timer as timer
import logging

# Setup Logging
# Gets or creates a logger
logger = logging.getLogger()

# set log level
logger.setLevel(logging.INFO)

# define file handler and set formatter
file_handler = logging.FileHandler('c:\\logs\\getclamps.log')
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s'
                              , datefmt='%m/%d/%Y %I:%M:%S %p')
file_handler.setFormatter(formatter)

# add file handler to logger
logger.addHandler(file_handler)

# Define Database Connection

CONNECTION = """
Driver={SQL Server};
Server=tn-sql;
Database=autodata;
autocommit=true;
UID=production;
PWD=Auto@matics;
"""

FORMAT = '%Y%m%d%H%M%S'


# Define some functions


def check_ping(ip):
    """Check if Machine is online"""
    cmd = Popen("ping -n 1 " + ip, stdout=PIPE)
    hoststate = "Unknown"
    for line in cmd.stdout:
        if "time" in str(line):
            hoststate = "Alive"
        if "unreachable" in str(line):
            hoststate = "Down"
        if "timed out" in str(line):
            hoststate = "Down"
    return hoststate


def update_machines(filename, path):
    """Update machines via FTP return list of unsucessful transfers."""
    # create dictionary structure for machine IP addresses
    machines = {'ACM365': '10.143.50.57', 'LACM387': '10.143.50.25',
                'ACM366': '10.143.50.59', 'ACM363': '10.143.50.55',
                'ACM362': '10.143.50.53', 'ACM369': '10.143.50.65',
                'SLACM392': '10.143.50.203', 'ACM353': '10.143.50.161',
                'ACM361': '10.143.50.51', 'ACM355': '10.143.50.165',
                'ACM354': '10.143.50.163', 'ACM351': '10.143.50.157',
                'ACM350': '10.143.50.155', 'LACM384': '10.143.50.21',
                'LACM383': '10.143.50.23', 'ACM372': '10.143.50.69',
                'ACM373': '10.178.4.111', 'ACM367': '10.143.50.61',
                'ACM374': '10.143.50.73', 'ACM375': '10.143.50.75',
                'LACM390': '10.143.50.27', 'LACM391': '10.143.50.31',
                'LACM385': '10.143.50.13', 'LACM381': '10.143.50.15',
                'LACM382': '10.143.50.17', 'LACM386': '10.143.50.19',
                'ACM376': '10.143.50.77', 'LACM388': '10.143.50.87',
                'CG002': '10.143.50.123', 'CG001': '10.143.50.121',
                'LACM393': '10.143.50.215', 'SLACM389': '10.143.50.128',
                'ACM356': '10.143.50.200'
                }
    badmachlist = []
    screwfile = 'screws.csv'
    newpartflag = 'newparts.txt'
    for mach, ip in list(machines.items()):
        status = check_ping(ip)
        if status != 'Alive':
            statedata = "Network Connection to " + mach + " at " + ip + " is " + status
            logger.warning(statedata)
            badmachlist.append(statedata)
            continue
        print("Transferring " + mach)
        try:
            start = timer()
            s = ftplib.FTP(ip, 'anonymous', 'anonymous')
            f = open(path + filename, "rb")
            print("Transferring " + filename + ' To ' + mach)
            s.storbinary('STOR ' + filename, f)
            f.close()
            f = open(path + screwfile, "rb")
            print("Transferring " + screwfile + ' To ' + mach)
            s.storbinary('STOR ' + screwfile, f)
            f.close()
            f = open(path + newpartflag, "rb")
            print("Transferring " + newpartflag + ' To ' + mach)
            s.storbinary('STOR ' + newpartflag, f)
            f.close()
            s.quit()
        except Exception as e:
            badmachlist.append(mach)
            msg = 'Machine Update Failed: ' + mach + ': ' + str(e) + " [" + update_machines.__name__ + "]"
            logger.error(msg)
            CommonFunc.send_text('sgilmo', ['sgilmo'], msg)
            print(msg)
        else:
            msg = "FTP Transfer Time for " + mach + " was " + str(round((timer() - start), 3)) + " sec"
            logger.info(msg)
            print(msg)
    print(badmachlist)
    return badmachlist


def save_history(filename, partpath, histpath):
    """Move file to history folder with timestamp as new name."""
    shutil.copy2(partpath + filename, histpath +
                 datetime.now().strftime(FORMAT) + ".csv")
    return


def log_bad_machines(machlist, path):
    """Create a log file of machines that had connection issues."""
    if not machlist:
        if os.path.isfile(path + "badlist.csv"):
            os.remove(path + "badlist.csv")
        return
    else:
        # create file for nodes that did not connect
        outputfile = open(path + "badlist.csv", "w", newline='')
        out = csv.writer(outputfile, delimiter=',', quoting=csv.QUOTE_NONE)
        out.writerows([machlist])
        outputfile.close()
        # Send the Email
        CommonFunc.send_email(machlist, "The Following Machine Connections Failed",
                              "The Following Machines are not Connecting to the network:\n\n")
    return


def log_changes(chglist, path):
    """Create log file for changes."""
    file_connect = open(path + "upd_" + datetime.now().strftime(FORMAT) + ".log", "w")
    # Add changed parts to file
    file_connect.write('Part numbers that were updated:\n')
    for part in chglist:
        file_connect.write(part + '\n')
    file_connect.close()
    return


def check_maint_files(mpath, qpath):
    """Check if Maint added any modified programs to server."""
    print('Checking for Maintenance Files')
    newfiles = []
    filelist = []
    try:
        for fname in os.listdir(mpath):
            if os.path.isfile(os.path.join(mpath, fname)):
                filelist.append(fname)
        for filename in filelist:
            print(filename)
            shutil.copy2(mpath + filename, qpath + filename)
            os.remove(mpath + filename)
            newfiles.append(filename)
        if len(newfiles) > 0:
            print(str(len(newfiles)) + ' Files Found')
            CommonFunc.send_email(newfiles, "File(s) Added to MAINT Directory",
                                  "The following file(s) need to be reviewed:\n\n")
            print('Done')
    except Exception as e:
        msg = 'Maint File Sweep Failed: ' + ': ' + str(e) + " [" + check_maint_files.__name__ + "]"
        logger.error(msg)
        CommonFunc.send_text('sgilmo', ['sgilmo'], msg)
        print(msg)
    return


def get_file_items(dbase, path):
    """If parts.csv does not exist, create from database.

    If file does exist, compare it with database.
    If different replace file with current data.
    """
    dskfile = []
    if not os.path.isfile(path + "parts.csv"):
        outputfile = open(path + "parts.csv", "w", newline='')
        out = csv.writer(outputfile, delimiter=',', quoting=csv.QUOTE_NONE)
        out.writerows(dbase)
        outputfile.close()
        dskfile = dbase
        logger.info("No Parts File, Building New Parts File")
    else:
        start = timer()
        inputfile = open(path + "parts.csv")
        reader = csv.reader(inputfile, delimiter=',', quoting=csv.QUOTE_NONE)
        for data in reader:
            dskfile.append(data)
        inputfile.close()
        print(str(len(dskfile)) + " CSV Records Processed in " + str(round((timer() - start), 3)) + " sec")
    return dskfile


def get_filemaker_items():
    """Get Current part data file from Filemaker database."""
    start = timer()
    cnxn = pyodbc.connect('DSN=FM Clamp ODBC;UID=FMODBC;PWD=FMODBC')
    cnxn.timeout = 60
    cursor = cnxn.cursor()
    print("FileMaker Connect Time = " + str(round((timer() - start), 3)) + " sec")
    print("Executing Query on FileMaker. Go get a beer, this will take a while!")
    sql = """
            SELECT Ourpart,"Band A Part Number", "Housing A Part Number",
                "Screw Part Number" AS Screw, "Band Feed from Band data",
                "Ship Diam Max", "Ship Diam Min", "Hex Size", "Band_Thickness",
                "Band_Width", "CameraInspectionRequired", "ScrDrvChk"
            FROM tbl8Tridon 
            WHERE  ("Band Feed from Band data" IS NOT NULL)
                AND (Ourpart IS NOT NULL) AND (RIGHT(Ourpart,1) <> '\r')
                AND ("Housing A Part Number" IS NOT NULL)
                AND (RIGHT("Housing A Part Number",1) <> '\r')
                AND ("Screw Part Number" IS NOT NULL)
                AND (RIGHT("Screw Part Number",1) <> '\r')
                AND ("Band A Part Number" IS NOT NULL)
                AND (RIGHT("Band A Part Number",1) <> '\r')
                AND ("Band_Thickness" IS NOT NULL)
                AND ("Band Width" IS NOT NULL)
                AND (CameraInspectionRequired IS NOT NULL)
                AND (ScrDrvChk IS NOT NULL)
                AND ("Hex Size" IS NOT NULL)
            ORDER BY Ourpart
        """
    try:
        start = timer()
        cursor.execute(sql)
        result = cursor.fetchall()
    except Exception as e:
        msg = 'FileMaker Query Failed: ' + str(e) + " [" + get_filemaker_items.__name__ + "]"
        logger.error(msg)
        result = []
        print(msg)
    else:
        msg = str(len(result)) + " FM Records Processed in " + str(round((timer() - start), 3)) + " sec."
        print(msg)

    dbase = []
    start = timer()
    for row in result:
        row = cleandata(row)
        dbase.append(list([str(x) for x in row]))
    cnxn.close()
    print("Data Clean Time = " + str(round((timer() - start), 3)) + " sec")
    return dbase


def update_db(dbase):
    """ Add part data to SQL server database"""
    dbcnxn = pyodbc.connect(CONNECTION)
    cursor = dbcnxn.cursor()
    cursor.fast_executemany = True
    start = timer()
    try:
        cursor.execute("TRUNCATE TABLE production.parts")
        dbcnxn.commit()
    except Exception as e:
        msg = 'MSSQL Table Deletion Failed: ' + str(e) + " [" + update_db.__name__ + "]"
        logger.error(msg)
    else:
        print("Delete Time = " + str(round((timer() - start), 3)) + " sec")
    # Load part data onto SQL server
    sql = """INSERT INTO production.parts (PartNumber,Band,Housing,Screw,Feed,
                    DiaMax,DiaMin,HexSz,BandThickness,BandWidth,CamInspect,ScrDrvChk)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?);"""
    try:
        start = timer()
        cursor.executemany(sql, dbase)
        dbcnxn.commit()
    except Exception as e:
        msg = 'MSSQL Table Update Failed: ' + str(e) + " [" + update_db.__name__ + "]"
        logger.error(msg)
        print(msg)
    else:
        print(str(len(dbase)) + " MSSQL Records Processed in " + str(round((timer() - start), 3)) + " sec")
    dbcnxn.close()
    return dbase


# noinspection DuplicatedCode,DuplicatedCode
def cleandata(row):
    """ Cleanup bad data in database row"""
    if row[5] == "":
        row[5] = '0.00'
    if row[6] == "":
        row[6] = '0.00'
    # Get rid of bad part number entry
    row[0] = row[0].replace('\xa0', '')
    # Get rid of leading and following spaces
    for x in range(11):
        try:
            row[x] = str(row[x]).strip()
        except Exception as e:
            msg = 'Record Stripping Failed: ' + str(e) + " [" + cleandata.__name__ + "]"
            logger.error(msg)
            print(msg)
    return CommonFunc.scrub_data(row)


def main():
    """Main Function."""
    # Define file paths
    partdatapath = "\\Inetpub\\ftproot\\acmparts\\"
    parthistpath = "\\Inetpub\\ftproot\\acmparts\\history\\"
    maintpath = "\\\\tn-san-fileserv\\Engineering Data\\Elab\\Machines\\MAINT\\"
    quepath = "\\\\tn-san-fileserv\\Engineering Data\\Elab\\Machines\\ReviewQue\\"

    start = timer()
    print('\n\n\n')
    # Load Filemaker Data Into SQL Server Table
    dbase = update_db(get_filemaker_items())

    # dskfile is data read from the parts.csv file the machines are using now
    dskfile = get_file_items(dbase, partdatapath)

    if dbase[0:] != dskfile[0:]:
        # print('Part file different, replacing part file and sending to machines')
        # Log differences
        diffs = [c[0] for c in dbase[0:] if c not in dskfile[0:]]
        num_diffs = str(len(diffs))
        print('Part file different (' + num_diffs + '), replacing part file and sending to machines')
        for x in diffs:
            print(x)
        log_changes(diffs, parthistpath)
        msg = "Number of Parts Modified, Added or Deleted = " + num_diffs
        logger.info(msg)
        print(msg)
        # Save Copy
        save_history("parts.csv", partdatapath, parthistpath)
        # Replace file
        outputfile = open(partdatapath + "parts.csv", "w", newline='')
        out = csv.writer(outputfile, delimiter=',', quoting=csv.QUOTE_NONE)
        out.writerows(dbase)
        outputfile.close()
        # Send to machines and log bad transfers
        # log_bad_machines(update_machines('parts.csv', partdatapath), partdatapath)
    else:
        print('Files Identical, No Need to Replace')
    # Check Maintenance Directory for New Files
    # check_maint_files(maintpath, quepath)
    msg = "Total Time = " + str(round((timer() - start), 3)) + " sec"
    logger.info(msg)
    print(msg)
    return


if __name__ == '__main__':
    main()
