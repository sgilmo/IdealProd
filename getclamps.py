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
import sys
import shutil
from datetime import datetime
from subprocess import Popen, PIPE
import pyodbc
import common_funcs
from timeit import default_timer as timer
import logging

# Setup Logging
# Gets or creates a logger
logger = logging.getLogger(__name__)

# set log level
logger.setLevel(logging.INFO)

# define file handler and set formatter
file_handler = logging.FileHandler('c:\\logs\\getclamps.log')
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
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

# Define Some Globals

# create dictionary structure for machine IP addresses
MACHINES = {'ACM365': '10.143.50.57', 'LACM387': '10.143.50.25', 'ACM366': '10.143.50.59',
            'ACM363': '10.143.50.55', 'ACM362': '10.143.50.53', 'ACM369': '10.143.50.65',
            'SLACM392': '10.143.50.203', 'ACM353': '10.143.50.161', 'ACM361': '10.143.50.51',
            'ACM355': '10.143.50.165', 'ACM354': '10.143.50.163', 'ACM351': '10.172.6.192',
            'ACM350': '10.143.50.155', 'LACM384': '10.143.50.21', 'LACM383': '10.143.50.23',
            'ACM372': '10.143.50.69', 'ACM374': '10.143.50.73', 'ACM357': '10.143.50.66',
            'ACM375': '10.143.50.75', 'LACM390': '10.143.50.27', 'LACM391': '10.143.50.31',
            'LACM385': '10.172.6.175', 'LACM381': '10.143.50.15', 'LACM382': '10.143.50.17',
            'LACM386': '10.143.50.19', 'ACM376': '10.143.50.77', 'LACM388': '10.143.50.87',
            'CG002': '10.143.50.123', 'CG001': '10.143.50.121', 'LACM393': '10.143.50.215',
            'SLACM389': '10.143.50.128', 'ACM367': '10.172.6.165', 'LACM394': '10.143.50.172',
            'ACM358': '10.143.50.195', 'ACM1193': '10.172.6.5','ACM1122': '10.172.6.194',
            'ACM1123': '10.172.6.199','ACM352': '10.143.50.133'
            }
GEN3 = ['ACM352','ACM357', 'ACM358', 'LACM394', 'ACM1193', 'ACM1122', 'ACM1123']

# Define some functions


def check_ping(ip):
    """Check if Machine is on the network"""
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


def valid_ips(d, keys):
    """Create new machine dictionary excluding offline machines"""
    return {x: d[x] for x in d if x not in keys}


def check_mach_conn():
    """Check Machine Network Connections"""
    badmachlist = []
    badmachmsg = []
    for mach, ip in list(MACHINES.items()):
        status = check_ping(ip)
        if status != 'Alive':
            statedata = "Network Connection to " + mach + " at " + ip + " is " + status
            badmachmsg.append(statedata)
            badmachlist.append(mach)
    if len(badmachlist) > 0:
        email_bad_machines(badmachmsg)
    new_mach_dict = valid_ips(MACHINES, badmachlist)
    return new_mach_dict


def update_machines(filename, path, mach_dict):
    """Update machines via FTP return list of unsucessful transfers."""
    screwfile = 'screws.csv'
    screwfile_job = 'screws.job'
    newpartflag = 'newparts.txt'
    unsuccessful_transfers = []

    files_to_transfer = [
        (filename, filename),
        (screwfile, screwfile_job),
        (newpartflag, newpartflag)
    ]

    for mach, ip in mach_dict.items():
        print(f"Transferring {mach}")
        start = timer()
        try:
            # Establish FTP connection
            with ftplib.FTP(ip, 'anonymous', 'anonymous') as ftp:
                for local_file, gen3_filename in files_to_transfer:
                    try:
                        with open(os.path.join(path, local_file), "rb") as f:
                            print(f"Transferring {local_file} to {mach}")
                            dest = f'STOR /MEMCARD1/{gen3_filename}' if mach in GEN3 else f'STOR {local_file}'
                            ftp.storbinary(dest, f)
                    except OSError as e:
                        logger.error(f"Error opening file {local_file} for {mach}: {str(e)}")
                        unsuccessful_transfers.append(mach)
                        break
        except ftplib.all_errors as e:
            msg = f'Machine Update Failed: {mach}: {str(e)} [{update_machines.__name__}]'
            logger.error(msg)
            print(msg)
            unsuccessful_transfers.append(mach)
        else:
            transfer_time = round(timer() - start, 3)
            msg = f'FTP Transfer Time for {mach} was {transfer_time} sec'
            logger.info(msg)
            print(msg)

    return unsuccessful_transfers


def save_history(filename, partpath, histpath):
    """Move file to history folder with timestamp as new name."""
    shutil.copy2(partpath + filename, histpath +
                 datetime.now().strftime(FORMAT) + ".csv")
    return


def email_bad_machines(machlist):
    """Create a log file of machines that had connection issues."""
    mailto = ["elab@idealtridon.com"]  # storing the receiver's mail id
    # Send the Email
    if len(machlist) > 1:
        subject = "The Following Machines Could Not Recieve New Part Data Files"
        header = "The Following Machines could not<br>be reached via FTP:\n\n"
    else:
        subject = "The Following Machine Could Not Recieve New Part Data Files"
        header = "The Following Machine could not<br>be reached via FTP:\n\n"
    common_funcs.build_email(machlist, subject, header, mailto)
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
            common_funcs.send_email(newfiles, "File(s) Added to MAINT Directory",
                                    "The following file(s) need to be reviewed:\n\n")
            print('Done')
    except Exception as e:
        msg = 'Maint File Sweep Failed: ' + ': ' + str(e) + " [" + check_maint_files.__name__ + "]"
        logger.error(msg)
        # common_funcs.send_text(msg)
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


def get_file_screws(path):
    """Get screw data from screws.csv and load it into dskfile.
    """
    dskfile = []
    start = timer()
    inputfile = open(path + "screws.csv")
    reader = csv.reader(inputfile, delimiter=',', quoting=csv.QUOTE_NONE)
    for data in reader:
        dskfile.append(data[:3])
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
                "Screw Part Number", "Band Feed from Band data",
                "Ship Diam Max QA Alternate", "Ship Diam Min", "Hex Size", "Band_Thickness", "Band_Width", 
                "CameraInspectionRequired", "ScrDrvChk"
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
        print(msg)
        # common_funcs.send_text(msg)
        sys.exit(1)
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
        # common_funcs.send_text(msg)
        sys.exit(1)
    else:
        print("Delete Time = " + str(round((timer() - start), 3)) + " sec")
    # Load part data onto SQL server
    sql = """INSERT INTO production.parts (PartNumber,Band,Housing,Screw,Feed,
                    DiaMax,DiaMin,HexSz,BandThickness,BandWidth,CamInspect,ScrDrvChk)
                    VALUES (?,?,?,?,?,?,?,?,?,round(?,3),?,?);"""
    try:
        start = timer()
        cursor.executemany(sql, dbase)
        dbcnxn.commit()
    except Exception as e:
        msg = 'MSSQL Table Update Failed: ' + str(e) + " [" + update_db.__name__ + "]"
        logger.error(msg)
        print(msg)
        # common_funcs.send_text(msg)
    else:
        print(str(len(dbase)) + " MSSQL Records Processed in " + str(round((timer() - start), 3)) + " sec")
    dbcnxn.close()
    return dbase


def update_scrdb(dbase):
    """ Add part data to SQL server database"""
    dbcnxn = pyodbc.connect(CONNECTION)
    cursor = dbcnxn.cursor()
    cursor.fast_executemany = True
    start = timer()
    try:
        cursor.execute("TRUNCATE TABLE production.screws")
        dbcnxn.commit()
    except Exception as e:
        msg = 'MSSQL Table Deletion Failed: ' + str(e) + " [" + update_scrdb.__name__ + "]"
        logger.error(msg)
        # common_funcs.send_text(msg)
        sys.exit(1)
    else:
        print("Delete Time = " + str(round((timer() - start), 3)) + " sec")
    # Load part data onto SQL server
    sql = """INSERT INTO production.screws (screw_num,screw_desc,screw_ht)
                    VALUES (?,?,round(?,3));"""
    try:
        start = timer()
        cursor.executemany(sql, dbase)
        dbcnxn.commit()
    except Exception as e:
        msg = 'MSSQL Table Update Failed: ' + str(e) + " [" + update_scrdb.__name__ + "]"
        logger.error(msg)
        print(msg)
        # common_funcs.send_text(msg)
    else:
        print(str(len(dbase)) + " MSSQL Records Processed in " + str(round((timer() - start), 3)) + " sec")
    dbcnxn.close()
    return


def cleandata(row):
    """ Cleanup bad data in database row"""
    # If Ship Dia is Null, make it 0.00
    if row[5] == "":
        row[5] = '0.00'
    if row[6] == "":
        row[6] = '0.00'
    # Get rid of bad part number entry
    row[0] = row[0].replace('\xa0', '')
    # Make sure Cam Inspect and Screwdriver Check field are all caps
    row[10] = row[10].upper()
    row[11] = row[11].upper()
    # Get rid of leading and following spaces
    for x in range(12):
        try:
            row[x] = str(row[x]).strip()
        except Exception as e:
            msg = 'Record Stripping Failed: ' + str(e) + " [" + cleandata.__name__ + "]"
            logger.error(msg)
            print(msg)
            # common_funcs.send_text(msg)
    return common_funcs.scrub_data(row)


def main():
    """Main Function."""
    # Define file paths
    partdatapath = "\\Inetpub\\ftproot\\acmparts\\"
    parthistpath = "\\Inetpub\\ftproot\\acmparts\\history\\"
    maintpath = "\\\\tn-san-fileserv\\dept\\Engineering\\Engineering Data\\Elab\\Machines\\MAINT\\"
    quepath = "\\\\tn-san-fileserv\\dept\\Engineering\\Engineering Data\\Elab\\Machines\\ReviewQue\\"

    start = timer()
    print('\n\n\n')
    # Load Filemaker Data Into SQL Server Table
    dbase = update_db(get_filemaker_items())

    # dskfile is data read from the parts.csv file the machines are using now
    dskfile = get_file_items(dbase, partdatapath)

    if dbase[0:] != dskfile[0:]:
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
        # Send to machines
        mach_dict = check_mach_conn()
        print(update_machines('parts.csv', partdatapath, mach_dict))
    else:
        print('Files Identical, No Need to Replace')
    # Check Maintenance Directory for New Files
    check_maint_files(maintpath, quepath)
    update_scrdb(get_file_screws(partdatapath))
    msg = "Total Time = " + str(round((timer() - start), 3)) + " sec"
    logger.info(msg)
    print(msg)
    return


if __name__ == '__main__':
    main()
