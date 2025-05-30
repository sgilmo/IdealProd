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
import shutil
import pandas as pd
from datetime import date
from datetime import timedelta
from pretty_html_table import build_table
import csv
from datetime import datetime
import sqlalchemy.exc
import faults
import pyodbc
import common_funcs
import logging
from timeit import default_timer as timer
from sqlalchemy import text
from sql_funcs import CONNECTION_STRING, engine

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

# Constants for paths
BASE_PATH = "\\Inetpub\\ftproot\\"
MAILTO = ["sgilmour@idealtridon.com"]

# Define some Functions
def _is_obsolete_file(file_name):
    """Check if a file is an obsolete packer or micro file."""
    return file_name.startswith(('2', '9')) and len(file_name) > 10


def _move_file(file_name, src_path, dest_path, action):
    """Move a file to a designated folder and log action."""
    shutil.move(f"{src_path}{file_name}", f"{dest_path}{file_name}")
    print(f'File {file_name} Moved to {action} Folder')


def _filter_data(dataframe, table_name):
    """Filter the DataFrame for the specific table based on rules."""
    if table_name in ["AcmRuntime", "tblStrut_Exp"]:
        return dataframe[dataframe['RecDate'].ne('0')]
    elif table_name == "MachProd":
        return dataframe[
            (dataframe['Total'] > 0) &
            (dataframe['Start'] != '0') &
            (dataframe['Stop'] != '0')
            ]
    elif table_name == "MachShifts":
        dataframe = _sanitize_shift_data(dataframe)
        return dataframe[
            (dataframe['ProdCnt'] > 0) &
            (dataframe['ShiftDate'] != '0') &
            (dataframe['ShiftRecDate'] != '0') &
            (dataframe['Shift'] > 0)
            ]
    elif table_name == "MachJobs":
        _fill_defaults(dataframe, {"SetupMan": "Unknown", "CoType": "No Changes"})
        return dataframe[
            (dataframe['JobCnt'] > 0) &
            (dataframe['JobComp'].ne('0')) &
            (dataframe['JobStart'].ne('0'))
            ]
    elif table_name == "opprod":
        dataframe = _sanitize_operator_data(dataframe)
        return dataframe[dataframe['login'] != '0']
    return dataframe


def _sanitize_shift_data(dataframe):
    """Sanitize MachShifts-specific data."""
    if not (common_funcs.check_for_int(dataframe.Eff) and
            common_funcs.check_for_int(dataframe.NetEff) and
            common_funcs.check_for_int(dataframe.Util)):
        # dataframe.update({"Eff": 0.00, "NetEff": 0.00, "Util": 0.00})
        dataframe.Eff = 0.00
        dataframe.NetEff = 0.00
        dataframe.Util = 0.0
    dataframe['Operator'].fillna("Unknown", inplace=True)
    dataframe['Operator'].replace(to_replace="Please login", value='Unknown', inplace=True)
    return dataframe


def _sanitize_operator_data(dataframe):
    """Sanitize opprod-specific data."""
    if not (common_funcs.check_for_int(dataframe.eff) and
            common_funcs.check_for_int(dataframe.neteff) and
            common_funcs.check_for_int(dataframe.util)):
        # dataframe.update({"eff": 0.00, "neteff": 0.00, "util": 0.00})
        dataframe.eff = 0.00
        dataframe.neteff = 0.00
        dataframe.util = 0.00
    return dataframe


def _fill_defaults(dataframe, default_values):
    """Fill default values in the DataFrame."""
    for column, default_value in default_values.items():
        dataframe[column].fillna(default_value, inplace=True)
        dataframe[column].replace(to_replace="Please login", value=default_value, inplace=True)


def _handle_file_error(exception, file_name, folder_paths, error_type):
    """Handle errors during file processing."""
    prob_desc = f'{file_name} Had A Problem Loading: Moving to Bad Folder'
    prob_detail = f'<b>Problem Detail: </b><br>{exception}'
    message_header = f'Problem With <i>{file_name}</i> : {error_type}'
    print(prob_desc)
    print(prob_detail)
    print(message_header)

    common_funcs.build_email2(prob_detail, prob_desc, message_header, MAILTO)
    shutil.move(f"{folder_paths['main']}{file_name}", f"{folder_paths['bad']}{file_name}")


def _handle_sql_error(exception, error_description):
    """Handle SQL-related exceptions."""
    prob_desc = f'Problem Sending Data to SQL Server Table ({error_description})'
    prob_detail = f'<b>Problem Detail: </b><br>{exception}'
    message_header = prob_desc
    print(prob_desc)
    print(prob_detail)
    common_funcs.build_email2(prob_detail, prob_desc, message_header, MAILTO)



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


# Collect Machine Production data
def load_db(folder_name, table_name, dtype_dict):
    """Load Operator Production Data into the SQL Server."""
    folder_paths = {
        "main": f"{BASE_PATH}{folder_name}\\",
        "bad": f"{BASE_PATH}{folder_name}_bad\\",
        "archive": f"{BASE_PATH}{folder_name}_archive\\",
        "obs": f"{BASE_PATH}obs_data\\",
    }
    # Ensure necessary directories exist
    for path in folder_paths.values():
        check_dir(path)

    # Initialize an empty DataFrame with correct dtypes
    processed_data = pd.DataFrame(columns=dtype_dict.keys()).astype(dtype_dict)

    # Get file list from the directory
    file_list = os.listdir(folder_paths["main"])
    if not file_list:
        print(f'Nothing to Process in Folder {folder_name}')
        return
    else:
        print(f'Processing Folder {folder_name}')

    # Process each file
    for file_name in file_list:
        if _is_obsolete_file(file_name):
            _move_file(file_name, folder_paths["main"], folder_paths["obs"], "Obsolete Data")
            continue

        try:
            print(f'Table {table_name} is being processed {file_name}')
            raw_data = pd.read_csv(f"{folder_paths['main']}{file_name}", names=dtype_dict.keys())
            filtered_data = _filter_data(raw_data, table_name)
            if not filtered_data.empty:
                processed_data = pd.concat([processed_data, filtered_data.astype(dtype_dict)])
        except (ValueError, Exception) as e:
            _handle_file_error(e, file_name, folder_paths, "DataFrame Loading Error")
        else:
            _move_file(file_name, folder_paths["main"], folder_paths["archive"], "Archived")

    # Reset index for the processed DataFrame
    processed_data.reset_index(drop=True, inplace=True)

    # Load data into SQL Server
    try:
        processed_data.to_sql(table_name, engine, schema='production', if_exists='append', index=False)
    except sqlalchemy.exc.IntegrityError as e:
        _handle_sql_error(e, "Duplicate Primary Key")
    except Exception as e:
        _handle_sql_error(e, "Other SQL Error")
    else:
        msg = f"SQL Server Table {table_name} Updated"
        print(msg)
        logger.info(msg)



def load_mach_prod(fpath, size):
    """Load Production Data into SQL Server."""
    # prodbadfilepath = "\\Inetpub\\ftproot\\machprodbad\\"
    dbcnxn = pyodbc.connect(CONNECTION_STRING)
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
                logger.error(msg + " [" + load_mach_prod.__name__ + "]")
                BAD_FILE_LIST.append("MachProd_" + filename)
                print(msg)
                continue
            except pyodbc.Error as e:
                print(rowdata)
                msg = "Bad Data In File: " + str(row[0]) + ": " + str(e)
                logger.error(msg + " [" + load_mach_prod.__name__ + "]")
                BAD_FILE_LIST.append("MachProd_" + filename)
                print(msg)
                continue
            else:
                print(row[3] + " Entered into machprod database")
        inputfile.close()
    dbcnxn.close()
    return


# noinspection DuplicatedCode
def load_mach_production(fpath, size):
    """Load Shift Production Data into SQL Server."""
    prodbadfilepath = "\\Inetpub\\ftproot\\acmprodbad\\"
    dbcnxn = pyodbc.connect(CONNECTION_STRING)
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
    dbcnxn = pyodbc.connect(CONNECTION_STRING)
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
    dbcnxn = pyodbc.connect(CONNECTION_STRING)
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


def checkins(dtypes):
    """report any machines that did not check in"""
    machs = ('ACM350', 'ACM351', 'ACM353', 'ACM354', 'ACM355', 'ACM361', 'ACM362',
             'ACM363', 'ACM365', 'ACM366', 'ACM367', 'ACM369', 'ACM372', 'ACM374', 'ACM375',
             'ACM376', 'LACM381', 'LACM382', 'LACM383', 'LACM384', 'LACM385', 'LACM386',
             'LACM387', 'LACM388', 'LACM390', 'LACM391', 'LACM393', 'SLACM389', 'SLACM392')
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
        if missed_machines > 0:
            return missing_mach
    return []


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
    # TODO: Get network permissions to move files to tooling drive
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


# TODO: Remove when use of load_db proves out
def process_production_file(file_path, file_name, cursor, db_connection, table_name, bad_file_path, expected_size):
    """Process a single production file and insert its data into the database."""
    full_path = os.path.join(file_path, file_name)
    with open(full_path, 'r') as input_file:
        parser = csv.reader(input_file)
        row = next(parser)

    print(f"Processing {table_name} Production File: {full_path}")
    if len(row) >= expected_size:
        sql = f"""INSERT INTO {table_name} 
                  (Datestamp,Machine,RecDate,hr0,hr1,hr2,hr3,hr4,hr5,hr6,hr7,hr8,hr9,
                   hr10,hr11,hr12,hr13,hr14,hr15,hr16,hr17,hr18,hr19,hr20,hr21,hr22,hr23)
                  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
        rows = tuple(row[:27])  # Safely limit rows to the first 27 elements
        try:
            cursor.execute(sql, rows)
            db_connection.commit()
        except pyodbc.IntegrityError:
            handle_error("Duplicate Primary Key", row[0], file_name, bad_file_path, full_path, table_name)
        except pyodbc.Error:
            handle_error("Could not load", row[0], file_name, bad_file_path, full_path, table_name)
        else:
            os.remove(full_path)
            print(f"{row[1]} {file_name} Entered into {table_name} production database")


def handle_error(message, key, file_name, bad_file_path, full_path, table_name):
    """Handle errors by logging and moving bad files."""
    full_message = f"{message} {key}. File = {file_name}"
    logger.error(f"{full_message} [process_production_file]")
    print(full_message)
    BAD_FILE_LIST.append(f"{table_name.lower()}_{file_name}")
    movefile(full_path, os.path.join(bad_file_path, file_name))


def load_production_data(fpath, size, table_name, bad_file_path, file_type):
    """Load Hourly Production Data into SQL Server."""
    db_connection = pyodbc.connect(CONNECTION_STRING)
    cursor = db_connection.cursor()

    check_file_size(fpath, file_type)  # Move zero size files to other directory
    file_list = os.listdir(fpath)

    for file_name in file_list:
        process_production_file(fpath, file_name, cursor, db_connection, table_name, bad_file_path, size)

    db_connection.close()


# Updated functions now use the generic `load_production_data`
def load_strut_production(fpath, size):
    load_production_data(
        fpath=fpath,
        size=size,
        table_name="production.tblStrut",
        bad_file_path="\\Inetpub\\ftproot\\Wesanco\\Badfiles\\",
        file_type="Strut"
    )

def load_fastlok_production(fpath, size):
    load_production_data(
        fpath=fpath,
        size=size,
        table_name="production.tblFastLok",
        bad_file_path="\\Inetpub\\ftproot\\FastLok\\Badfiles\\",
        file_type="FastLok"
    )

# def load_strut_production(fpath, size):
#     """Load Hourly Strut Production Data into SQL Server."""
#     strutbadfilepath = "\\Inetpub\\ftproot\\Wesanco\\Badfiles\\"
#     dbcnxn = pyodbc.connect(CONNECTION_STRING)
#     cursor = dbcnxn.cursor()
#     # Loads shift data to SQL server
#     check_file_size(fpath, "Strut")  # Move zero size files to other directory
#     filelist = os.listdir(fpath)
#     machlist = []
#     for filename in filelist:
#         inputfile = open(fpath + filename)
#         parser = csv.reader(inputfile)
#         row = next(parser)
#         machlist.append(row[1])
#         inputfile.close()
#         print("Processing Strut Production File: " + fpath + filename)
#         if len(row) >= size:
#             sql = """INSERT INTO production.tblStrut (Datestamp,Machine,RecDate,hr0,hr1,hr2,hr3,hr4,hr5,hr6,hr7,hr8,hr9,
#                                  hr10,hr11,hr12,hr13,hr14,hr15,hr16,hr17,hr18,hr19,hr20,hr21,hr22,hr23)
#                      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
#             rows = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
#                     row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18],
#                     row[19], row[20], row[21], row[22], row[23], row[24], row[25], row[26])
#             try:
#                 cursor.execute(sql, rows)
#                 dbcnxn.commit()
#             except pyodbc.IntegrityError:
#                 msg = "Duplicate Primary Key " + str(row[0]) + "File = " + filename
#                 logger.error(msg + " [" + load_strut_production.__name__ + "]")
#                 print(msg)
#                 BAD_FILE_LIST.append("strut_" + filename)
#                 movefile(fpath + filename, strutbadfilepath + filename)
#             except pyodbc.Error:
#                 msg = "could not load " + filename
#                 logger.error(msg + " [" + load_strut_production.__name__ + "]")
#                 print(msg)
#                 BAD_FILE_LIST.append("strut_" + filename)
#                 movefile(fpath + filename, strutbadfilepath + filename)
#             else:
#                 # os.remove(fpath + filename)
#                 print(row[1] + filename + " Entered into strut production database")
#     dbcnxn.close()
#     return
#
#
# # TODO: Add to Pandas Data Model
# def load_fastlok_production(fpath, size):
#     """Load Hourly FastLok Production Data into SQL Server."""
#     fastlokbadfilepath = "\\Inetpub\\ftproot\\FastLok\\Badfiles\\"
#     dbcnxn = pyodbc.connect(CONNECTION_STRING)
#     cursor = dbcnxn.cursor()
#     # Loads shift data to SQL server
#     check_file_size(fpath, "FastLok")  # Move zero size files to other directory
#     filelist = os.listdir(fpath)
#     machlist = []
#     for filename in filelist:
#         inputfile = open(fpath + filename)
#         parser = csv.reader(inputfile)
#         row = next(parser)
#         machlist.append(row[1])
#         inputfile.close()
#         print("Processing FastLok Production File: " + fpath + filename)
#         if len(row) >= size:
#             sql = """INSERT INTO production.tblFastLok (Datestamp,Machine,RecDate,hr0,hr1,hr2,hr3,hr4,hr5,hr6,
#                                  hr7,hr8,hr9,hr10,hr11,hr12,hr13,hr14,hr15,hr16,hr17,hr18,hr19,hr20,hr21,hr22,hr23)
#                      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
#             rows = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
#                     row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18],
#                     row[19], row[20], row[21], row[22], row[23], row[24], row[25], row[26])
#             try:
#                 cursor.execute(sql, rows)
#                 dbcnxn.commit()
#             except pyodbc.IntegrityError:
#                 msg = "Duplicate Primary Key " + str(row[0]) + "File = " + filename
#                 logger.error(msg + " [" + load_fastlok_production.__name__ + "]")
#                 print(msg)
#                 BAD_FILE_LIST.append("fastlok_" + filename)
#                 movefile(fpath + filename, fastlokbadfilepath + filename)
#             except pyodbc.Error:
#                 msg = "could not load " + filename
#                 logger.error(msg + " [" + load_fastlok_production.__name__ + "]")
#                 print(msg)
#                 BAD_FILE_LIST.append("fastlok_" + filename)
#                 movefile(fpath + filename, fastlokbadfilepath + filename)
#             else:
#                 os.remove(fpath + filename)
#                 print(row[1] + filename + " Entered into FastLok production database")
#     dbcnxn.close()
#     return


def get_uptime():
    today = date.today()
    yesterday = today - timedelta(days=1)
    strsql = """SELECT AcmRuntime.Machine,
                    ROUND(
                    CAST((production.AcmRuntime.hr0 + production.AcmRuntime.hr1 + production.AcmRuntime.hr2 +
                    production.AcmRuntime.hr3 + production.AcmRuntime.hr4 + production.AcmRuntime.hr5 +
                    production.AcmRuntime.hr6 + production.AcmRuntime.hr7 + production.AcmRuntime.hr8 +
                    production.AcmRuntime.hr9 + production.AcmRuntime.hr10 + production.AcmRuntime.hr11 +
                    production.AcmRuntime.hr12 + production.AcmRuntime.hr13 + production.AcmRuntime.hr14 +
                    production.AcmRuntime.hr15 + production.AcmRuntime.hr16 + production.AcmRuntime.hr17 +
                    production.AcmRuntime.hr18 + production.AcmRuntime.hr19 + production.AcmRuntime.hr20 +
                    production.AcmRuntime.hr21 + production.AcmRuntime.hr22 + production.AcmRuntime.hr23) AS FLOAT)/864,2)
                    AS Uptime
                    FROM [production].[AcmRuntime]
                    WHERE
                        AcmRuntime.RecDate >= '%s'
                    ;""" % yesterday
    with engine.connect() as connection:
        df1 = pd.read_sql(text(strsql), connection)
    return df1.sort_values('Machine')


def uptime_rpt():
    mach_data = get_uptime()
    # mailto = ["elab@idealtridon.com", "bbrackman@idealtridon.com",
    #           "thobbs@idealtridon.com", "mpriddy@idealtridon.com"]
    mailto = ["sgilmour@idealtridon.com"]
    # For Debug
    # mailto = ["sgilmour@idealtridon.com"]

    # List of Active Machines that are supposed to submit production data
    machs = ('ACM350', 'ACM352', 'ACM353', 'ACM354', 'ACM355', 'ACM357', 'ACM358', 'ACM361', 'ACM362',
             'ACM363', 'ACM365', 'ACM366', 'ACM369', 'ACM372', 'ACM374', 'ACM375',
             'ACM376', 'LACM381', 'LACM382', 'LACM383', 'LACM384', 'LACM386', 'LACM387',
             'LACM388', 'LACM390', 'LACM391', 'LACM393', 'LACM394', 'SLACM389', 'SLACM392')

    missing_mach = set(machs).difference(mach_data['Machine'])
    data = build_table(mach_data[['Machine', 'Uptime']], 'blue_light')
    yesterday = date.today() - timedelta(days=1)
    # logger.info('Sending Email in uptime report')
    if len(missing_mach) == 1:
        data = data + '<br><br><i>Machine That Did Not Check In: ' + ' '.join(missing_mach) + '</i>'
    if len(missing_mach) > 1:
        data = data + '<br><br><i>Machines That Did Not Check In: ' + ', '.join(missing_mach) + '</i>'
    common_funcs.build_email2(data, 'Uptime Report', 'ACM Uptime Report for ' + str(yesterday), mailto)


def main():
    """Main Function."""
    # Set some paths
    # TODO: Delete unused paths when converted to Pandas data model (using load_db())
    shipdiapath = "\\Inetpub\\ftproot\\acmtests\\ShipDia\\"
    thickpath = "\\Inetpub\\ftproot\\acmtests\\Thickness\\"
    strut1path = "\\Inetpub\\ftproot\\Wesanco\\Weld1\\"
    strut2path = "\\Inetpub\\ftproot\\Wesanco\\Weld2\\"
    strut5path = "\\Inetpub\\ftproot\\Wesanco\\Weld4\\"
    strut4path = "\\Inetpub\\ftproot\\FlaStrut\\Weld1\\"
    fastlok1path = "\\Inetpub\\ftproot\\FastLok\\FL2874\\"
    fastlok2path = "\\Inetpub\\ftproot\\FastLok\\FL2874-2\\"

    start = timer()
    logger.info("Program Started")

    # Collect Test data
    # TODO: Convert to Pandas data model (using load_db())
    log_test_data(thickpath, "TG")
    log_test_data(shipdiapath, "CG")

    # Collect Machine Production data
    logger.info('Running MachProd')
    dtypes = {"ID": 'int64', "Part": 'object', "Operator": 'object', "Machine": 'category', "Start": 'datetime64[ns]',
              "Stop": 'datetime64[ns]', "Shift": 'int', "Total": 'int', "CO": 'int', "PPM": 'int'}
    load_db('machprod', 'MachProd', dtypes)

    # Collect Shift Data
    logger.info('Running MachShifts')
    dtypes = {"ID": 'int64', "ShiftDate": 'datetime64[ns]', "ShiftRecDate": 'datetime64[ns]', "Machine": 'object',
              "Operator": 'object', "ProdCnt": 'int', "Eff": 'float64', "NetEff": 'float64', "Util": 'float64',
              "Shift": 'int'}
    load_db('acmshift', 'MachShifts', dtypes)

    # Collect Job Data
    logger.info('Running MachJobs')
    dtypes = {"ID": 'int64', "JobComp": 'datetime64[ns]', "JobStart": 'datetime64[ns]', "Machine": 'object',
              "PartNum": 'object', "JobCnt": 'int', "CoTime": 'int', "CoType": 'object', "SetupMan": 'object'}
    load_db('acmjob', 'MachJobs', dtypes)

    # Collect Operator Production Data
    logger.info('Running OppProd')
    dtypes = {"ID": 'int64', "part": 'object', "operator": 'object', "machine": 'object', "login": 'datetime64[ns]',
              "logout": 'datetime64[ns]', "shift": 'int', "production": 'int', "eff": 'float', "neteff": 'float',
              "util": 'float'}
    load_db('opprod', 'opprod', dtypes)

    # Collect Hourly Runtime Data ACMs
    logger.info('Running Runtime for ACMs')
    dtypes = {"ID": 'int64', "Machine": 'object', "RecDate": 'datetime64[ns]', "hr0": 'int', "hr1": 'int',
              "hr2": 'int', "hr3": 'int', "hr4": 'int', "hr5": 'int', "hr6": 'int', "hr7": 'int', "hr8": 'int',
              "hr9": 'int', "hr10": 'int', "hr11": 'int', "hr12": 'int', "hr13": 'int', "hr14": 'int', "hr15": 'int',
              "hr16": 'int', "hr17": 'int', "hr18": 'int', "hr19": 'int', "hr20": 'int', "hr21": 'int', "hr22": 'int',
              "hr23": 'int'}
    load_db('acmrtdata', 'AcmRuntime', dtypes)

    logger.info('Running Wesanco Strut Welder 1')
    load_strut_production(strut1path, 20)
    logger.info('Running Wesanco Strut Welder 2')
    load_strut_production(strut2path, 20)
    logger.info('Running Wesanco Strut Welder 4')
    load_strut_production(strut5path, 20)
    logger.info('Running Florida Strut Welder 1')
    load_strut_production(strut4path, 20)

    # Collect Hourly Runtime Data for Struts
    # Pandas Model
    dtypes = {"Datestamp": 'int', "Machine": 'object', "RecDate": 'datetime64[ns]', "hr0": 'int',
              "hr1": 'float', "hr2": 'float', "hr3": 'float', "hr4": 'float', "hr5": 'float', "hr6": 'float',
              "hr7": 'float', "hr8": 'float', "hr9": 'float', "hr10": 'float', "hr11": 'float', "hr12": 'float',
              "hr13": 'float', "hr14": 'float', "hr15": 'float', "hr16": 'float', "hr17": 'float', "hr18": 'float',
              "hr19": 'float', "hr20": 'float', "hr21": 'float', "hr22": 'float', "hr23": 'float'}
    logger.info('Running Runtime for Wesanco Strut Welder 1')
    load_db('Wesanco\\Weld1', 'tblStrut_Exp', dtypes)
    logger.info('Running Runtime for Wesanco Strut Welder 2')
    load_db('Wesanco\\Weld2', 'tblStrut_Exp', dtypes)
    # logger.info('Running Runtime for Wesanco Strut Welder 3')
    # load_db('Wesanco\\Weld3', 'tblStrut_Exp1', dtypes)
    logger.info('Running Runtime for Wesanco Strut Welder 4')
    load_db('Wesanco\\Weld4', 'tblStrut_Exp', dtypes)
    logger.info('Running Runtime for Florida Strut Welder 1')
    load_db('FlaStrut\\Weld1', 'tblStrut_Exp', dtypes)
    # Generate Uptime Report Email
    # logger.info('Running Uptime Report')
    # uptime_rpt()

    # TODO: Remove when converted to Pandas data model
    logger.info('Running FastLok 1')
    load_fastlok_production(fastlok1path, 20)
    logger.info('Running FastLok 2')
    load_fastlok_production(fastlok2path, 20)
    logger.info('Running check_badfile')
    check_badfile()
    logger.info('Running Get Faults')
    faults.get_faults()

    # TODO: Convert to Pandas data model (using load_db())
    set_cam_files()
    logger.info('Running log_conegage_data')
    log_conegage_data()
    logger.info("Total Execution Time = " + str(round((timer() - start), 3)) + " sec")
    return


if __name__ == '__main__':
    main()
