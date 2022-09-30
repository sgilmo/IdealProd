# coding=utf-8
# !/usr/bin/env python


"""This Program checks target directories for data files.

When files are found they are parsed and their data is inserted into the
sql server database there are four file types one for shift data that will be generated.
at every shift change and one for job data that will be generated during a.
changeover one for production time/attendance data and one for Ship.
Diameter test results.
"""


import os
import sys
import shutil
import urllib
import pandas as pd
from datetime import date
from datetime import timedelta
from pretty_html_table import build_table
from sqlalchemy import create_engine
import csv
import datetime
import sqlalchemy.exc
# from datetime import datetime
import faults
import pyodbc
import common_funcs
import logging
from timeit import default_timer as timer

# Setup Logging
# Gets or creates a logger
logger = logging.getLogger()

# set log level
logger.setLevel(logging.INFO)

# define file handler and set formatter
file_handler = logging.FileHandler('c:\\logs\\update.log')
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
file_handler.setFormatter(formatter)


# add file handler to logger
logger.addHandler(file_handler)

# initialize bad file list for emailing files that could not be transferred
BAD_FILE_LIST = []

# Define Database ConnectionS
# SQL Server
CONNECTION_MSSQL = """
Driver={ODBC Driver 17 for SQL Server};
Server=tn-sql;
Database=autodata;
autocommit=true;
UID=production;
PWD=Auto@matics;
"""

# SQLAlchemy connection
server = 'tn-sql'
database = 'autodata'
driver = 'ODBC+Driver+17+for+SQL+Server'
user = 'production'
pwd = urllib.parse.quote_plus("Auto@matics")
port = '1433'
database_conn = f'mssql+pyodbc://{user}:{pwd}@{server}:{port}/{database}?driver={driver}'
# Make Connection
engine = create_engine(database_conn)
conn = engine.raw_connection()
# cursor = conn.cursor()


# Define some Functions
def check_dir(directory):
    """Create Directories As Needed"""
    folder = os.path.isdir(directory)
    # If folder doesn't exist, then create it.
    if not folder:
        os.makedirs(directory)
    return


def move_cam_files(fpath, dest):
    """Move Camera Files to Engineering Server"""
    # Move Camera JPG files
    types = ("jpg", "bmp")
    dirs = os.listdir(fpath)
    i = 0
    for item in dirs:
        if item[-3:] in types:
            create_time = os.path.getctime(fpath + item)
            strdate = datetime.datetime.fromtimestamp(create_time).strftime('%Y-%m-%d')
            yr = datetime.datetime.fromtimestamp(create_time).strftime('%Y')
            mnth_num = datetime.datetime.fromtimestamp(create_time).strftime('%m')
            mnth_short = datetime.datetime.fromtimestamp(create_time).strftime('%b')
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


# Collect Machine Production data
def load_db(folder_name, table_name, dtype_dict):
    """Load Operator Production Data into the SQL Server"""
    fpath = "\\Inetpub\\ftproot\\" + folder_name + "\\"
    check_dir(fpath)
    fpath_bad = "\\Inetpub\\ftproot\\" + folder_name + "_bad\\"
    check_dir(fpath_bad)
    fpath_arc = "\\Inetpub\\ftproot\\" + folder_name + "_archive\\"
    check_dir(fpath_arc)
    fpath_obs = "\\Inetpub\\ftproot\\obs_data\\"
    check_dir(fpath_obs)
    # Initialize DataFrames
    df1 = pd.DataFrame(columns=dtype_dict.keys())
    df1 = df1.astype(dtype_dict)
    df3 = pd.DataFrame(columns=dtype_dict.keys())
    df3 = df1.astype(dtype_dict)
    # Get Files From Server
    filelist = os.listdir(fpath)
    # If no files, exit
    if len(filelist) == 0:
        print('Nothing to Process in Folder ' + folder_name)
        return
    else:
        print('Processing Folder ' + folder_name)
    for filename in filelist:
        if (filename[:1] == '2') or (filename[:1] == '9'):
            shutil.move(fpath + filename, fpath_obs + filename)
            print('File ' + filename + ' Moved to ' + fpath_obs)
            continue
        try:
            print('Table ' + table_name + ' is being processed ' + filename)
            df = pd.read_csv(fpath + filename, names=dtype_dict.keys())
            # Filter for Bad Data
            if table_name == 'MachProd':
                df3 = (df['Part'] != 'None') & (df['Start'] != '0') & (df['Stop'] != '0')
            elif table_name == 'MachShifts':
                df['Operator'].fillna("Unknown", inplace=True)
                df['Operator'].replace(to_replace="Please login", value='Unknown', inplace=True)
                df3 = (df['ProdCnt'] > 0) & (df['ShiftDate'] != '0') & (df['ShiftRecDate'] != '0') & (df['Shift'] > 0)
            elif table_name == 'MachJobs':
                df['SetupMan'].fillna("Unknown", inplace=True)
                df['CoType'].fillna("No Changes", inplace=True)
                df['SetupMan'].replace(to_replace="Please login", value='Unknown', inplace=True)
                df3 = (df['JobCnt'] > 0) & (df['JobComp'] != '0') & (df['JobStart'] != '0')
            elif table_name == 'AcmRuntime':
                df3 = df['RecDate'] != '0'
            elif table_name == 'opprod':
                df3 = (df['login'] != '0')
            df = df.loc[df3]
            df = df.astype(dtype_dict)
            df1 = pd.concat([df1, df])
        except ValueError as e:
            print(filename + ' Had A Value Error Problem Loading Into DataFrame: Moving to ' + fpath_bad)
            print('Problem Detail: ' + str(e))
            shutil.move(fpath + filename, fpath_bad + filename)
        except Exception as e:
            print(filename + ' Had A Problem Loading Into DataFrame: Moving to ' + fpath_bad)
            print('Problem Detail: ' + str(e))
            shutil.move(fpath + filename, fpath_bad + filename)
        else:
            # Copy Data File to Archive Folder
            shutil.move(fpath + filename, fpath_arc + filename)
            print(filename + ' Loaded Into DataFrame')
    df1.reset_index(drop=True, inplace=True)
    try:
        # Send Data to SQL Server
        df1.to_sql(table_name, engine, schema='production', if_exists='append', index=False)
    except sqlalchemy.exc.IntegrityError as e:
        prob_desc = 'Problem Sending Data to SQL Server Table (Duplicate Primary Key)'
        prob_detail = '<b>Problem Detail: </b><br>' + str(e)
        print(prob_desc)
        print(prob_detail)
        message_header = 'Problem With <i>' + filename + '</i> :'
        common_funcs.send_email_progflt(prob_detail, prob_desc, message_header)
    except Exception as e:
        print('Problem Sending Data to SQL Server Table')
        print('Problem Detail: ' + str(e))
    else:
        print('SQL Server Table ' + table_name + ' Updated')


def load_mach_prod(fpath, size):
    """Load Production Data into SQL Server."""
    # prodbadfilepath = "\\Inetpub\\ftproot\\machprodbad\\"
    dbcnxn = pyodbc.connect(CONNECTION_MSSQL)
    cursor = dbcnxn.cursor()
    # Loads production data to SQL server
    check_file_size(fpath, "Prod")  # Move zero size files to other directory
    filelist = os.listdir(fpath)
    for filename in filelist:
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
            except sqlalchemy.exc.IntegrityError:
                msg = "Duplicate Primary Key " + str(row[0]) + " The Offending File is " + filename
                logger.error(msg + " [" + sys._getframe(0).f_code.co_name + "]")
                BAD_FILE_LIST.append("MachProd_" + filename)
                print(msg)
                continue
            except pyodbc.Error as e:
                print(rowdata)
                msg = "Bad Data In File: " + str(row[0]) + ": " + str(e)
                logger.error(msg + " [" + sys._getframe(0).f_code.co_name + "]")
                BAD_FILE_LIST.append("MachProd_" + filename)
                print(msg)
                continue
            else:
                print(row[3] + " Entered into machprod database")
        inputfile.close()
    dbcnxn.close()
    return


def load_mach_shifts(fpath, size):
    """Load Shift Data into SQL Server."""
    shiftbadfilepath = "\\Inetpub\\ftproot\\acmshiftbad\\"
    dbcnxn = pyodbc.connect(CONNECTION_MSSQL)
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
        if int(row[5]) <= 20:
            os.remove(fpath + filename)
            continue
        # Test That Data is Numeric for appropriate fields
        if not common_funcs.check_for_int(row[5]):
            msg = "Non Numeric Value for Shift Count: " + row[5] + ": File = " + filename
            logger.error(msg + " [" + load_mach_shifts.__name__ + "]")
            movefile(fpath + filename, shiftbadfilepath + filename)
            continue
        if not common_funcs.check_for_int(row[6]):
            msg = "Non Numeric Value for Eff: " + row[6] + ": File = " + filename
            logger.error(msg + " [" + load_mach_shifts.__name__ + "]")
            movefile(fpath + filename, shiftbadfilepath + filename)
            continue
        if not common_funcs.check_for_int(row[7]):
            msg = "Non Numeric Value for Net Eff: " + row[7] + ": File = " + filename
            logger.error(msg + " [" + load_mach_shifts.__name__ + "]")
            movefile(fpath + filename, shiftbadfilepath + filename)
            continue
        if not common_funcs.check_for_int(row[8]):
            msg = "Non Numeric Value for Utilization: " + row[8] + ": File = " + filename
            logger.error(msg + " [" + load_mach_shifts.__name__ + "]")
            movefile(fpath + filename, shiftbadfilepath + filename)
            continue
        if not common_funcs.check_for_int(row[9]):
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
        # os.remove(fpath + filename)
    dbcnxn.close()
    return


def load_mach_jobs(fpath, size):
    """Load Job Data into SQL Server."""
    jobbadfilepath = "\\Inetpub\\ftproot\\acmjobbad\\"
    dbcnxn = pyodbc.connect(CONNECTION_MSSQL)
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
        # os.remove(fpath + filename)
    dbcnxn.close()
    return


def load_mach_runtime(fpath, size):
    """Load Shift Data into SQL Server."""
    # Check for Machine Check ins
    checkins()
    rtbadfilepath = "\\Inetpub\\ftproot\\acmrtbad\\"
    dbcnxn = pyodbc.connect(CONNECTION_MSSQL)
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
                # os.remove(fpath + filename)
                # fpath_arc = "\\Inetpub\\ftproot\\acmrtdata_archive\\"
                # shutil.move(fpath + filename, fpath_arc + filename)
                msg = row[1] + " Entered into runtime database"
                print(msg)
                logger.info(msg)
    dbcnxn.close()
    return


# noinspection DuplicatedCode
def load_mach_production(fpath, size):
    """Load Shift Production Data into SQL Server."""
    prodbadfilepath = "\\Inetpub\\ftproot\\acmprodbad\\"
    dbcnxn = pyodbc.connect(CONNECTION_MSSQL)
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
    dbcnxn = pyodbc.connect(CONNECTION_MSSQL)
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
    dbcnxn = pyodbc.connect(CONNECTION_MSSQL)
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
        common_funcs.send_email(BAD_FILE_LIST, "Database Entry Failed", strdesc)
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


def checkins():
    """report any machines that did not check in"""
    machs = ('ACM350', 'ACM351', 'ACM353', 'ACM354', 'ACM355', 'ACM356', 'ACM361', 'ACM362',
             'ACM363', 'ACM365', 'ACM366', 'ACM367', 'ACM369', 'ACM372', 'ACM374', 'ACM375',
             'ACM376', 'LACM381', 'LACM382', 'LACM383', 'LACM384', 'LACM385', 'LACM386',
             'LACM387', 'LACM388', 'LACM390', 'LACM391', 'LACM393', 'SLACM389', 'SLACM392')
    dtypes = {"ID": 'int64', "Machine": 'object', "RecDate": 'datetime64', "hr0": 'int', "hr1": 'int',
              "hr2": 'int', "hr3": 'int', "hr4": 'int', "hr5": 'int', "hr6": 'int', "hr7": 'int', "hr8": 'int',
              "hr9": 'int', "hr10": 'int', "hr11": 'int', "hr12": 'int', "hr13": 'int', "hr14": 'int', "hr15": 'int',
              "hr16": 'int', "hr17": 'int', "hr18": 'int', "hr19": 'int', "hr20": 'int', "hr21": 'int', "hr22": 'int',
              "hr23": 'int'}
    fpath = "\\Inetpub\\ftproot\\acmrtdata\\"

    filelist = os.listdir(fpath)
    if len(filelist) > 0:
        df1 = pd.read_csv(fpath + filelist[0], names=dtypes.keys())
        df1 = df1.astype(dtypes)
        for filename in filelist[1:]:
            df = pd.read_csv(fpath + filename, names=dtypes.keys())
            df = df.astype(dtypes)
            df1 = pd.concat([df1, df])
        missing_mach = set(machs).difference(df1['Machine'])
        missed_machines = len(missing_mach)
        subject = "Machines Not Checking In"
        msg_header = "The Following " + str(missed_machines) + " Machines Did Not Check In:<br>"
        if missed_machines == 1:
            msg_header = "The Following Machine Did Not Check In:<br>"
        if missed_machines > 0:
            common_funcs.send_email_mach(missing_mach, subject, msg_header)
    return


def movefile(oldfile, newfile):
    """Move file to new location, file name needs to contain path"""
    try:
        os.rename(oldfile, newfile)
    except OSError as e:
        # os.remove(oldfile)
        msg = "File Move Error: " + str(e) + ": FILE NOT MOVED"
        print(msg)
        logger.error(msg + " [" + movefile.__name__ + "]")


def set_cam_files():
    """Move Camera Files to Server from Machines Equipped With Cameras"""
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
        dest = "\\\\tn-san-fileserv\\TOOLING\\2874 PREFORM CLAMP AUTOMATION\\Vision\\Lok Images\\Bad\\"
        move_cam_files(fpath, dest)
        fpath = "\\inetpub\\ftproot\\acmlogs\\" + mach + "\\PassedLoks\\"
        if not os.path.isdir(fpath):
            os.mkdir(fpath)
        dest = "\\\\tn-san-fileserv\\TOOLING\\2874 PREFORM CLAMP AUTOMATION\\Vision\\Lok Images\\Good\\"
        move_cam_files(fpath, dest)


def load_strut_production(fpath, size):
    """Load Hourly Strut Production Data into SQL Server."""
    strutbadfilepath = "\\Inetpub\\ftproot\\Wesanco\\Badfiles\\"
    dbcnxn = pyodbc.connect(CONNECTION_MSSQL)
    cursor = dbcnxn.cursor()
    # Loads shift data to SQL server
    check_file_size(fpath, "Strut")  # Move zero size files to other directory
    filelist = os.listdir(fpath)
    machlist = []
    for filename in filelist:
        inputfile = open(fpath + filename)
        parser = csv.reader(inputfile)
        row = next(parser)
        machlist.append(row[1])
        inputfile.close()
        print("Processing Strut Production File: " + fpath + filename)
        if len(row) >= size:
            sql = """INSERT INTO production.tblStrut (Datestamp,Machine,RecDate,hr0,hr1,hr2,hr3,hr4,hr5,hr6,hr7,hr8,hr9,
                                 hr10,hr11,hr12,hr13,hr14,hr15,hr16,hr17,hr18,hr19,hr20,hr21,hr22,hr23)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
            try:
                cursor.execute(sql, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                                     row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18],
                                     row[19], row[20], row[21], row[22], row[23], row[24], row[25], row[26]))
                dbcnxn.commit()
            except pyodbc.IntegrityError:
                msg = "Duplicate Primary Key " + str(row[0]) + "File = " + filename
                logger.error(msg + " [" + load_strut_production.__name__ + "]")
                print(msg)
                BAD_FILE_LIST.append("strut_" + filename)
                movefile(fpath + filename, strutbadfilepath + filename)
            except pyodbc.Error:
                msg = "could not load " + filename
                logger.error(msg + " [" + load_strut_production.__name__ + "]")
                print(msg)
                BAD_FILE_LIST.append("strut_" + filename)
                movefile(fpath + filename, strutbadfilepath + filename)
            else:
                os.remove(fpath + filename)
                print(row[1] + filename + " Entered into strut production database")
    dbcnxn.close()
    return


def load_fastlok_production(fpath, size):
    """Load Hourly FastLok Production Data into SQL Server."""
    fastlokbadfilepath = "\\Inetpub\\ftproot\\FastLok\\Badfiles\\"
    dbcnxn = pyodbc.connect(CONNECTION_MSSQL)
    cursor = dbcnxn.cursor()
    # Loads shift data to SQL server
    check_file_size(fpath, "FastLok")  # Move zero size files to other directory
    filelist = os.listdir(fpath)
    machlist = []
    for filename in filelist:
        inputfile = open(fpath + filename)
        parser = csv.reader(inputfile)
        row = next(parser)
        machlist.append(row[1])
        inputfile.close()
        print("Processing FastLok Production File: " + fpath + filename)
        if len(row) >= size:
            sql = """INSERT INTO production.tblFastLok (Datestamp,Machine,RecDate,hr0,hr1,hr2,hr3,hr4,hr5,hr6,
                                 hr7,hr8,hr9,hr10,hr11,hr12,hr13,hr14,hr15,hr16,hr17,hr18,hr19,hr20,hr21,hr22,hr23)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
            try:
                cursor.execute(sql, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                                     row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18],
                                     row[19], row[20], row[21], row[22], row[23], row[24], row[25], row[26]))
                dbcnxn.commit()
            except pyodbc.IntegrityError:
                msg = "Duplicate Primary Key " + str(row[0]) + "File = " + filename
                logger.error(msg + " [" + load_fastlok_production.__name__ + "]")
                print(msg)
                BAD_FILE_LIST.append("fastlok_" + filename)
                movefile(fpath + filename, fastlokbadfilepath + filename)
            except pyodbc.Error:
                msg = "could not load " + filename
                logger.error(msg + " [" + load_fastlok_production.__name__ + "]")
                print(msg)
                BAD_FILE_LIST.append("fastlok_" + filename)
                movefile(fpath + filename, fastlokbadfilepath + filename)
            else:
                os.remove(fpath + filename)
                print(row[1] + filename + " Entered into FastLok production database")
    dbcnxn.close()
    return


def uptime_rpt():
    dtypes = {"ID": 'int64', "Machine": 'object', "RecDate": 'datetime64', "hr0": 'int', "hr1": 'int',
              "hr2": 'int', "hr3": 'int', "hr4": 'int', "hr5": 'int', "hr6": 'int', "hr7": 'int', "hr8": 'int',
              "hr9": 'int', "hr10": 'int', "hr11": 'int', "hr12": 'int', "hr13": 'int', "hr14": 'int', "hr15": 'int',
              "hr16": 'int', "hr17": 'int', "hr18": 'int', "hr19": 'int', "hr20": 'int', "hr21": 'int', "hr22": 'int',
              "hr23": 'int'}
    fpath = "\\Inetpub\\ftproot\\acmrtdata\\"

    filelist = os.listdir(fpath)
    if len(filelist) == 0:
        return
    if len(filelist) > 1:
        file_start = 1
        df1 = pd.read_csv(fpath + filelist[0], names=dtypes.keys())
        df1 = df1.astype(dtypes)
    else:
        file_start = 0
        df1 = pd.DataFrame(columns=dtypes.keys())
        df1 = df1.astype(dtypes)
    for filename in filelist[file_start:]:
        df = pd.read_csv(fpath + filename, names=dtypes.keys())
        df = df.astype(dtypes)
        df1 = pd.concat([df1, df])
    idx = df1.columns.str.startswith('hr')
    df1['Uptime'] = (df1.iloc[:, idx].sum(axis=1) / 86400) * 100.00
    pd.options.display.float_format = '{:.2f}'.format
    df1.reset_index(drop=True, inplace=True)
    df_data = df1[['Machine', 'Uptime']]
    data = build_table(df_data, 'blue_light')
    yesterday = date.today() - timedelta(days=1)
    common_funcs.send_email_uptime(data, 'Uptime Report', 'Uptime Report for ' + str(yesterday))
    return


def main():
    """Main Function."""
    # Set some paths
    # jobfilepath = "\\Inetpub\\ftproot\\acmjob\\"
    # shiftfilepath = "\\Inetpub\\ftproot\\acmshift\\"
    # prodfilepath = "\\Inetpub\\ftproot\\machprod\\"
    shipdiapath = "\\Inetpub\\ftproot\\acmtests\\ShipDia\\"
    thickpath = "\\Inetpub\\ftproot\\acmtests\\Thickness\\"
    # runtimedatapath = "\\Inetpub\\ftproot\\acmrtdata\\"
    # proddatapath = "\\Inetpub\\ftproot\\acmproddata\\"
    strut1path = "\\Inetpub\\ftproot\\Wesanco\\Weld1\\"
    strut2path = "\\Inetpub\\ftproot\\Wesanco\\Weld2\\"
    strut3path = "\\Inetpub\\ftproot\\Wesanco\\Weld3\\"
    strut4path = "\\Inetpub\\ftproot\\FlaStrut\\Weld1\\"
    fastlok1path = "\\Inetpub\\ftproot\\FastLok\\FL2874\\"
    fastlok2path = "\\Inetpub\\ftproot\\FastLok\\FL2874-2\\"

    start = timer()
    logger.info("Program Started")

    # Collect Test data
    log_test_data(thickpath, "TG")
    log_test_data(shipdiapath, "CG")

    # load_mach_shifts(shiftfilepath, 10)
    # load_mach_jobs(jobfilepath, 9)
    # load_mach_runtime(runtimedatapath, 20)

    # Generate Checkin Report Email
    checkins()
    # Generate Uptime Report Email
    uptime_rpt()
    # Collect Machine Production data
    dtypes = {"ID": 'int64', "Part": 'object', "Operator": 'object', "Machine": 'category', "Start": 'datetime64[ns]',
              "Stop": 'datetime64[ns]', "Shift": 'int', "Total": 'int', "PPM": 'int', "CO": 'int'}
    load_db('machprod', 'MachProd', dtypes)

    # Collect Shift Data
    dtypes = {"ID": 'int64', "ShiftDate": 'datetime64[ns]', "ShiftRecDate": 'datetime64[ns]', "Machine": 'object',
              "Operator": 'object',
              "ProdCnt": 'int', "Eff": 'float64', "NetEff": 'float64', "Util": 'float64', "Shift": 'int'}
    load_db('acmshift', 'MachShifts', dtypes)

    # Collect Job Data
    dtypes = {"ID": 'int64', "JobComp": 'datetime64[ns]', "JobStart": 'datetime64[ns]', "Machine": 'object',
              "PartNum": 'object', "JobCnt": 'int', "CoTime": 'int', "CoType": 'object', "SetupMan": 'object'}
    load_db('acmjob', 'MachJobs', dtypes)

    # Collect Operator Production Data
    dtypes = {"ID": 'object', "part": 'object', "operator": 'object', "machine": 'object', "login": 'datetime64[ns]',
              "logout": 'datetime64[ns]', "shift": 'int', "production": 'int', "eff": 'float', "neteff": 'float',
              "util": 'float'}
    load_db('opprod', 'opprod', dtypes)

    # Collect Hourly Runtime Data
    dtypes = {"ID": 'int64', "Machine": 'object', "RecDate": 'datetime64[ns]', "hr0": 'int', "hr1": 'int',
              "hr2": 'int', "hr3": 'int', "hr4": 'int', "hr5": 'int', "hr6": 'int', "hr7": 'int', "hr8": 'int',
              "hr9": 'int', "hr10": 'int', "hr11": 'int', "hr12": 'int', "hr13": 'int', "hr14": 'int', "hr15": 'int',
              "hr16": 'int', "hr17": 'int', "hr18": 'int', "hr19": 'int', "hr20": 'int', "hr21": 'int', "hr22": 'int',
              "hr23": 'int'}
    load_db('acmrtdata', 'AcmRuntime', dtypes)

    # load_mach_production(proddatapath, 20)
    load_strut_production(strut1path, 20)
    load_strut_production(strut2path, 20)
    load_strut_production(strut3path, 20)
    load_strut_production(strut4path, 20)
    load_fastlok_production(fastlok1path, 20)
    load_fastlok_production(fastlok2path, 20)

    check_badfile()

    faults.get_faults()

    # set_cam_files()

    log_conegage_data()
    logger.info("Total Execution Time = " + str(round((timer() - start), 3)) + " sec")
    return


if __name__ == '__main__':
    main()
