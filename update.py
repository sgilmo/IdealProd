# coding=utf-8
# !/usr/bin/env python


"""This Program checks target directories for data files.

When files are found they are parsed and their data is inserted into the
database there are four file types one for shift data that will be generated.
at every shift change and one for job data that will be generated during a.
changeover one for production time/attendance data and one for Ship.
Diameter test results.
"""


import os
import shutil
import csv
from datetime import datetime
import faults
import pyodbc
import platform
import CommonFunc
import logging
from timeit import default_timer as timer

# Setup Logging
# Gets or creates a logger
logger = logging.getLogger()

# set log level
logger.setLevel(logging.INFO)

# define file handler and set formatter
file_handler = logging.FileHandler('c:\\logs\\update.log')
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s'
                              , datefmt='%m/%d/%Y %I:%M:%S %p')
file_handler.setFormatter(formatter)

# add file handler to logger
logger.addHandler(file_handler)

# initialize bad file list for emailing files that could not be transferred
BAD_FILE_LIST = []

# Define Database Connection

CONNECTION = """
Driver={SQL Server Native Client 11.0};
Server=tn-sql14;
Database=autodata;
autocommit=true;
UID=production;
PWD=Auto@matics;
"""
if platform.release() == 'XP':
    CONNECTION = """
    Driver={SQL Server Native Client 10.0};
    Server=tn-sql14;
    Database=autodata;
    autocommit=true;
    UID=production;
    PWD=Auto@matics;
    """

# Define some Functions


def move_cam_files(fpath, dest):
    """Move Camera Files to Engineering Server"""
    # Move Camera JPG files
    types = ("jpg", "bmp")
    dirs = os.listdir(fpath)
    i = 0
    for item in dirs:
        if item[-3:] in types:
            create_time = os.path.getctime(fpath + item)
            strdate = datetime.fromtimestamp(create_time).strftime('%Y-%m-%d')
            yr = datetime.fromtimestamp(create_time).strftime('%Y')
            mnth_num = datetime.fromtimestamp(create_time).strftime('%m')
            mnth_short = datetime.fromtimestamp(create_time).strftime('%b')
            dest_full = dest + yr + "\\" + mnth_num + " - " + mnth_short + "\\"
            full_path = dest_full + strdate
            if not os.path.isdir(dest):
                os.mkdir(dest)
            if not os.path.isdir(dest + yr + "\\"):
                os.mkdir(dest + yr + "\\")
            if not os.path.isdir(dest_full):
                os.mkdir(dest_full)
            if not os.path.isdir(full_path):
                os.mkdir(full_path)
            print('Moving Camera File: ', item)
            shutil.move(fpath + item, full_path + "\\" + item)
            i += 1
    if i > 0:
        logger.info(str(i) + " Camera File(s) Moved for " + fpath)
    return


def load_mach_prod(fpath, size):
    """Load Production Data into SQL Server."""
    prodbadfilepath = "\\Inetpub\\ftproot\\machprodbad\\"
    dbcnxn = pyodbc.connect(CONNECTION)
    cursor = dbcnxn.cursor()
    # Loads production data to SQL server
    check_file_size(fpath, "Prod")  # Move zero size files to other directory
    filelist = os.listdir(fpath)
    for filename in filelist:
        badfile = 0
        inputfile = open(fpath + filename)
        parser = csv.reader(inputfile)
        for row in parser:
            print("Processing Prod File: " + fpath + filename)
            if len(row) < size:
                logger.error("Row Size Too Small: [" + str(len(row)) + "] For " + str(row[0])
                             + " [" + load_mach_prod.__name__ + "]")
                continue

            # Only record when production is greater then 25
            if int(row[7]) > 25:
                sql = """INSERT INTO production.MachProd (ID,Part,Operator,Machine,Start,Stop,Shift,Total,CO,PPM)
                         VALUES (?,?,?,?,?,?,?,?,?,?);"""
                rowdata = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
            else:
                continue

            try:
                cursor.execute(sql, rowdata)
                dbcnxn.commit()
            except pyodbc.IntegrityError:
                msg = "Duplicate Primary Key " + str(row[0]) + " The Offending File is " + filename
                logger.error(msg + " [" + load_mach_prod.__name__ + "]")
                BAD_FILE_LIST.append("MachProd_" + filename)
                print(msg)
                badfile = 1
                continue
            except pyodbc.Error as e:
                msg = "Bad Data In File: " + str(row[0]) + ": " + str(e)
                logger.error(msg + " [" + load_mach_prod.__name__ + "]")
                BAD_FILE_LIST.append("MachProd_" + filename)
                print(msg)
                badfile = 1
                continue
            else:
                print(row[3] + " Entered into machprod database")
        inputfile.close()
        if badfile == 0:
            os.remove(fpath + filename)
        else:
            movefile(fpath + filename, prodbadfilepath + filename)
    dbcnxn.close()
    return


def load_mach_shifts(fpath, size):
    """Load Shift Data into SQL Server."""
    shiftbadfilepath = "\\Inetpub\\ftproot\\acmshiftbad\\"
    dbcnxn = pyodbc.connect(CONNECTION)
    cursor = dbcnxn.cursor()
    # Loads shift data to SQL server
    check_file_size(fpath, "Shift")  # Move zero size files to other directory
    filelist = os.listdir(fpath)
    for filename in filelist:
        inputfile = open(fpath + filename)
        parser = csv.reader(inputfile)
        row = next(parser)
        inputfile.close()
        print("Processing Shift File: " + fpath + filename)
        # Make sure there was some production
        if int(row[5]) <= 0:
            os.remove(fpath + filename)
            continue
        # Test That Data is Numeric for appropriate fields
        if not CommonFunc.check_for_int(row[5]):
            msg = "Non Numeric Value for Shift Count: " + row[5] + ": File = " + filename
            logger.error(msg + " [" + load_mach_shifts.__name__ + "]")
            movefile(fpath + filename, shiftbadfilepath + filename)
            continue
        if not CommonFunc.check_for_int(row[6]):
            msg = "Non Numeric Value for Eff: " + row[6] + ": File = " + filename
            logger.error(msg + " [" + load_mach_shifts.__name__ + "]")
            movefile(fpath + filename, shiftbadfilepath + filename)
            continue
        if not CommonFunc.check_for_int(row[7]):
            msg = "Non Numeric Value for Net Eff: " + row[7] + ": File = " + filename
            logger.error(msg + " [" + load_mach_shifts.__name__ + "]")
            movefile(fpath + filename, shiftbadfilepath + filename)
            continue
        if not CommonFunc.check_for_int(row[8]):
            msg = "Non Numeric Value for Utilization: " + row[8] + ": File = " + filename
            logger.error(msg + " [" + load_mach_shifts.__name__ + "]")
            movefile(fpath + filename, shiftbadfilepath + filename)
            continue
        if not CommonFunc.check_for_int(row[9]):
            msg = "Non Numeric Value for Shift: " + row[9] + ": File = " + filename
            logger.error(msg + " [" + load_mach_shifts.__name__ + "]")
            movefile(fpath + filename, shiftbadfilepath + filename)
            continue

        if len(row) >= size:
            sql = """INSERT INTO production.MachShifts (ID,ShiftDate,ShiftRecDate,Machine,Operator,ProdCnt,Eff,
                                 NetEff,Util,Shift)
                     VALUES (?,?,?,?,?,?,?,?,?,?);"""
            try:
                cursor.execute(sql, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]))
                dbcnxn.commit()
            except pyodbc.IntegrityError:
                msg = "Duplicate Primary Key " + str(row[0]) + "File = " + filename
                logger.error(msg + " [" + load_mach_shifts.__name__ + "]")
                BAD_FILE_LIST.append("MachShifts_" + filename)
                movefile(fpath + filename, shiftbadfilepath + filename)
            except pyodbc.Error as e:
                msg = "could not load " + filename + ": " + str(e)
                logger.error(msg + " [" + load_mach_shifts.__name__ + "]")
                BAD_FILE_LIST.append("MachShifts_" + filename)
                movefile(fpath + filename, shiftbadfilepath + filename)
            else:
                # os.remove(path + filename)
                print(row[3] + " Entered into shift production database")
        os.remove(fpath + filename)
    dbcnxn.close()
    return


def load_mach_jobs(fpath, size):
    """Load Job Data into SQL Server."""
    jobbadfilepath = "\\Inetpub\\ftproot\\acmjobbad\\"
    dbcnxn = pyodbc.connect(CONNECTION)
    cursor = dbcnxn.cursor()
    # Loads job data to SQL server
    check_file_size(fpath, "Jobs")  # Move zero size files to other directory
    filelist = os.listdir(fpath)
    for filename in filelist:
        inputfile = open(fpath + filename)
        parser = csv.reader(inputfile)
        row = next(parser)
        inputfile.close()
        print("Processing Job File: " + fpath + filename)
        if len(row) < size:
            continue

        if int(row[5]) > 0:
            sql = """INSERT INTO production.MachJobs (ID,JobComp,JobStart,Machine,PartNum,JobCnt,COTime,COType,SetupMan)
                     VALUES (?,?,?,?,?,?,?,?,?);"""
            try:
                cursor.execute(sql, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
                dbcnxn.commit()
            except pyodbc.IntegrityError:
                msg = "Duplicate Primary Key " + str(row[0]) + "File = " + filename
                logger.error(msg + " [" + load_mach_jobs.__name__ + "]")
                print(msg)
                BAD_FILE_LIST.append("MachJobs_" + filename)
                movefile(fpath + filename, jobbadfilepath + filename)
            except pyodbc.Error as e:
                msg = "could not load " + filename + ": " + str(e)
                logger.error(msg + " [" + load_mach_jobs.__name__ + "]")
                print(msg)
                BAD_FILE_LIST.append("MachJobs_" + filename)
                movefile(fpath + filename, jobbadfilepath + filename)
            else:
                print(row[3] + " Entered into job production database")
        os.remove(fpath + filename)
    dbcnxn.close()
    return


# noinspection DuplicatedCode
def load_mach_runtime(fpath, size):
    """Load Shift Data into SQL Server."""
    rtbadfilepath = "\\Inetpub\\ftproot\\acmrtbad\\"
    dbcnxn = pyodbc.connect(CONNECTION)
    cursor = dbcnxn.cursor()
    # Loads shift data to SQL server
    check_file_size(fpath, "Runtime")  # Move zero size files to other directory
    filelist = os.listdir(fpath)
    machlist = []
    for filename in filelist:
        inputfile = open(fpath + filename)
        parser = csv.reader(inputfile)
        row = next(parser)
        machlist.append(row[1])
        inputfile.close()
        print("Processing Runtime File: " + fpath + filename)
        if len(row) >= size:
            sql = """INSERT INTO production.AcmRuntime (ID,Machine,RecDate,hr0,hr1,hr2,hr3,hr4,hr5,hr6,hr7,hr8,hr9,
                                 hr10,hr11,hr12,hr13,hr14,hr15,hr16,hr17,hr18,hr19,hr20,hr21,hr22,hr23)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
            try:
                cursor.execute(sql, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                                     row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18],
                                     row[19], row[20], row[21], row[22], row[23], row[24], row[25], row[26]))
                dbcnxn.commit()
            except pyodbc.IntegrityError:
                msg = "Duplicate Primary Key " + str(row[0]) + "File = " + filename
                logger.error(msg + " [" + load_mach_runtime.__name__ + "]")
                print(msg)
                BAD_FILE_LIST.append("ACMrt_" + filename)
                movefile(fpath + filename, rtbadfilepath + filename)
            except pyodbc.Error:
                msg = "could not load " + filename
                logger.error(msg + " [" + load_mach_runtime.__name__ + "]")
                print(msg)
                BAD_FILE_LIST.append("ACMrt_" + filename)
                movefile(fpath + filename, rtbadfilepath + filename)
            else:
                os.remove(fpath + filename)
                print(row[1] + " Entered into runtime database")
    dbcnxn.close()
    checkins(machlist)
    return


# noinspection DuplicatedCode
def load_mach_production(fpath, size):
    """Load Shift Production Data into SQL Server."""
    prodbadfilepath = "\\Inetpub\\ftproot\\acmprodbad\\"
    dbcnxn = pyodbc.connect(CONNECTION)
    cursor = dbcnxn.cursor()
    # Loads shift data to SQL server
    check_file_size(fpath, "Production")  # Move zero size files to other directory
    filelist = os.listdir(fpath)
    for filename in filelist:
        inputfile = open(fpath + filename)
        parser = csv.reader(inputfile)
        row = next(parser)
        inputfile.close()
        print("Processing Production File: " + fpath + filename)
        if len(row) >= size:
            sql = """INSERT INTO production.AcmProduction (ID,Machine,RecDate,hr0,hr1,hr2,hr3,hr4,hr5,hr6,hr7,hr8,hr9,
                                 hr10,hr11,hr12,hr13,hr14,hr15,hr16,hr17,hr18,hr19,hr20,hr21,hr22,hr23)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
            try:
                cursor.execute(sql, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                                     row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18],
                                     row[19], row[20], row[21], row[22], row[23], row[24], row[25], row[26]))
                dbcnxn.commit()
            except pyodbc.IntegrityError:
                msg = "Duplicate Primary Key " + str(row[0]) + "File = " + filename
                logger.error(msg + " [" + load_mach_production.__name__ + "]")
                print(msg)
                BAD_FILE_LIST.append("ACMprod_" + filename)
                movefile(fpath + filename, prodbadfilepath + filename)
            except pyodbc.Error:
                msg = "could not load " + filename
                logger.error(msg + " [" + load_mach_production.__name__ + "]")
                print(msg)
                BAD_FILE_LIST.append("ACMprod_" + filename)
                movefile(fpath + filename, prodbadfilepath + filename)
            else:
                os.remove(fpath + filename)
                print(row[1] + " Entered into production database")
    dbcnxn.close()
    return


def log_bad_row(badrow, dirname, desc):
    """Log Bad Row Reads."""
    badrowfilepath = "\\Inetpub\\ftproot\\" + dirname + "\\"
    dtformat = '%Y%m%d%H%M%S'
    # Log bad data rows
    filename = open(badrowfilepath + "bad_" +
                    datetime.now().strftime(dtformat) + ".log", "a")
    strdata = ','.join([str(z) for z in badrow])
    strdata = strdata + "\n" + desc + "\n"
    filename.write(strdata)
    filename.close()
    return


def log_test_data(fpath, test_type):
    """Log Ship Diameter Tests to SQL Server."""
    testbadfilepath = "\\Inetpub\\ftproot\\acmtestbad\\"
    dbcnxn = pyodbc.connect(CONNECTION)
    cursor = dbcnxn.cursor()
    filepath = fpath + "\\"
    filelist = os.listdir(filepath)
    for filename in filelist:
        badfile = 0
        inputfile = open(filepath + filename)
        parser = csv.reader(inputfile)
        for row in parser:
            sql = ""
            rowdata = ()
            if test_type == 'CG':
                print("Processing Ship Dia data file: " + filepath + filename)
                sql = """INSERT INTO production.AcmShipDia (ID,Tests,Machine,MachIP,TestReq,Operator,Part,DiaMin,
                                    DiaMax,TestComp,Reading)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
                rowdata = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10])
            elif test_type == 'TG':
                print("Processing Thickness Test data file: " + filepath + filename)
                sql = """INSERT INTO production.AcmThick (ID,Machine,MachIP,TestReq,Operator,Part,Spec,TestComp,Reading)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"""
                rowdata = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8])

            try:
                cursor.execute(sql, rowdata)
                dbcnxn.commit()
            except pyodbc.DatabaseError:
                msg = "Could not load into database: " + filename
                logger.error("Database Error: " + str(row[0]) + ": " + msg)
                badfile = 1
                BAD_FILE_LIST.append("LogTest_" + filename)
                print(msg)
                log_bad_row(row, "acmtestbad", msg)
            else:
                print("Row Entered into database")

        inputfile.close()
        if badfile == 0:
            os.remove(filepath + filename)
        elif badfile == 1:
            BAD_FILE_LIST.append("ACMTest_" + filename)
            movefile(fpath + filename, testbadfilepath + filename)
            print("Could not load " + filename)
    dbcnxn.close()
    return


def log_conegage_data():
    """Log Ship Diameter Tests to SQL Server."""
    filepath = "\\Inetpub\\ftproot\\conegage\\"
    dbcnxn = pyodbc.connect(CONNECTION)
    cursor = dbcnxn.cursor()
    filelist = os.listdir(filepath)
    for filename in filelist:
        inputfile = open(filepath + filename)
        parser = csv.reader(inputfile)
        for row in parser:
            sql = ""
            rowdata = ()
            if filename[:7] == 'shipdia':
                print("Processing Ship Dia data file: " + filepath + filename)
                sql = """INSERT INTO production.cgage_ShipDia (DateTime,ID,TestLog_NumTests,
                         TestLog_Mach,TestLog_ReqTime,
                         TestLog_Operator,TestLog_Part,TestLog_DiaMin,
                         TestLog_DiaMax,TestLog_TestComp,TestLog_Reading)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
                rowdata = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10])
            elif filename[:5] == 'thick':
                print("Processing Thickness Test data file: " + filepath + filename)
                sql = """INSERT INTO production.cgage_Thick (DateTime,ID,TestLog_Mach,TestLog_ReqTime,TestLog_Operator,
                         TestLog_Part,TestLog_Thickness,TestLog_TestComp,TestLog_Reading)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"""
                rowdata = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8])

            try:
                cursor.execute(sql, rowdata)
                dbcnxn.commit()
            except Exception as e:
                msg = 'MSSQL Table Update Failed: ' + str(e) + " [" + log_conegage_data.__name__ + "]"
                print(msg)
                logger.error("Database Error: " + str(row[0]) + ": " + msg)
            else:
                print("Row Entered into database")

        inputfile.close()
        os.remove(filepath + filename)
    dbcnxn.close()
    return


def check_badfile():
    """Check for bad file."""
    # If there are problem files, let us know.
    if len(BAD_FILE_LIST) > 0:
        if len(BAD_FILE_LIST) > 1:
            strdesc = "The Following Files Could Not Be Added To Database:\n\n"
        else:
            strdesc = "The Following File Could Not Be Added To Database:\n\n"
        CommonFunc.send_email(BAD_FILE_LIST, "Database Entry Failed", strdesc)
    return


def check_file_size(srcpath, ftype):
    """Check the file size."""
    # Checks file size in directory listing
    # Moves zero length files to alternate directory
    # for later inspection
    zerolenfiles = "\\Inetpub\\ftproot\\acmzero\\"
    filelist = (filename for filename in os.listdir(srcpath)
                if os.path.isfile(os.path.join(srcpath, filename)))
    for item in filelist:
        statinfo = os.stat(srcpath + item)
        if statinfo.st_size == 0:
            BAD_FILE_LIST.append("ZeroLen_" + ftype + "_" + item)
            shutil.move(srcpath + item, zerolenfiles)
    return


def checkins(activemach):
    """report any machines that did not check in"""
    # Define current machine list
    machines = ['ACM365', 'LACM387', 'ACM366', 'ACM362', 'ACM369', 'SLACM392', 'ACM353', 'ACM361', 'ACM355',
                'ACM354', 'ACM351', 'ACM350', 'LACM384', 'LACM383', 'ACM372', 'ACM373', 'ACM367',
                'ACM374', 'ACM375', 'LACM390', 'LACM391', 'LACM385', 'LACM381', 'LACM382', 'LACM386', 'ACM376',
                'LACM388', 'LACM393', 'SLACM389', 'ACM363']
    missingmach = set(machines).difference(activemach)
    missed_machines = len(missingmach)
    if missed_machines > 0:
        msg = 'The Following (' + str(missed_machines) + ') machines did not check in: ' + \
              str(missingmach) + " [" + checkins.__name__ + "]"
        logger.warning(msg)
        print(msg)


def movefile(oldfile, newfile):
    """Move file to new location, file name needs to contain path"""
    try:
        os.rename(oldfile, newfile)
    except OSError as e:
        os.remove(oldfile)
        msg = "File Move Error: " + str(e) + ": FILE DELETED"
        print(msg)
        logger.error(msg + " [" + movefile.__name__ + "]")


def main():
    """Main Function."""
    # Set some paths
    jobfilepath = "\\Inetpub\\ftproot\\acmjob\\"
    shiftfilepath = "\\Inetpub\\ftproot\\acmshift\\"
    prodfilepath = "\\Inetpub\\ftproot\\machprod\\"
    shipdiapath = "\\Inetpub\\ftproot\\acmtests\\ShipDia\\"
    thickpath = "\\Inetpub\\ftproot\\acmtests\\Thickness\\"
    runtimedatapath = "\\Inetpub\\ftproot\\acmrtdata\\"
    proddatapath = "\\Inetpub\\ftproot\\acmproddata\\"

    start = timer()
    logger.info("Program Started")

    # Collect Test data
    log_test_data(thickpath, "TG")
    log_test_data(shipdiapath, "CG")

    # Collect Production data
    load_mach_prod(prodfilepath, 10)
    load_mach_shifts(shiftfilepath, 10)
    load_mach_jobs(jobfilepath, 9)
    load_mach_runtime(runtimedatapath, 20)
    load_mach_production(proddatapath, 20)
    check_badfile()
    faults.get_faults()
    # Move Camera Files to Server from Machines Equipped With Cameras
    acms = ('LACM384', 'ACM367', 'LACM386', 'ACM372', 'LACM383', 'LACM385', 'LACM391', 'ACM366',
            'LACM382', 'LACM381', 'ACM362', 'LACM390', 'ACM373', 'ACM374', 'ACM375', 'ACM376', 'ACM361', 'ACM365')
    fastlok = ('FL2874', 'FL2874_2')
    for mach in acms:
        fpath = "\\inetpub\\ftproot\\acmlogs\\" + mach + "\\FailedScrews\\"
        if not os.path.isdir(fpath):
            os.mkdir(fpath)
        dest = "\\\\tn-san-fileserv\\TOOLING\\2794 ACM Screw Head Vision\\Camera\\Screw Images\\Bad\\"
        move_cam_files(fpath, dest)
    for mach in fastlok:
        fpath = "\\inetpub\\ftproot\\acmlogs\\" + mach + "\\FailedLoks\\"
        if not os.path.isdir(fpath):
            os.mkdir(fpath)
        dest = "\\\\tn-san-fileserv\\TOOLING\\2874 FASTLOK AUTOMATION\\Vision\\Lok Images\\Bad\\"
        move_cam_files(fpath, dest)
        fpath = "\\inetpub\\ftproot\\acmlogs\\" + mach + "\\PassedLoks\\"
        if not os.path.isdir(fpath):
            os.mkdir(fpath)
        dest = "\\\\tn-san-fileserv\\TOOLING\\2874 FASTLOK AUTOMATION\\Vision\\Lok Images\\Good\\"
        move_cam_files(fpath, dest)

    log_conegage_data()
    logger.info("Total Execution Time = " + str(round((timer() - start), 3)) + " sec")
    return


if __name__ == '__main__':
    main()
