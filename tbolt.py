# coding=utf-8
# !/usr/bin/env python
"""This Program opens up the Tbolt table in NewIdealCurrentSQL and parses it copying
relevant records to a table in the database Autodata.
"""

import pyodbc
import common_funcs
from timeit import default_timer as timer
import logging
import sys

# Setup Logging
# Gets or creates a logger
logger = logging.getLogger()

# set log level
logger.setLevel(logging.INFO)

# define file handler and set formatter
file_handler = logging.FileHandler('c:\\logs\\tbolt.log')
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
file_handler.setFormatter(formatter)

# add file handler to logger
logger.addHandler(file_handler)

# Define Database Connections

CONN_ENGDB = """
Driver={SQL Server Native Client 11.0};
Server=tn-sql14;
Database=autodata;
autocommit=true;
UID=production;
PWD=Auto@matics;
"""

CONN_PRODDB = """
Driver={SQL Server Native Client 11.0};
Server=tn-sql14;
Database=NewIdealCurrentSQL;
autocommit=true;
UID=production;
PWD=Auto@matics;
"""


FORMAT = '%Y%m%d%H%M%S'


# noinspection PyProtectedMember,PyUnresolvedReferences
def get_complist(comp_name):
    """Get Current band list from Prod Eng database."""
    print(f'Getting Unique {comp_name} Numbers From Product Table')
    cnxn = pyodbc.connect(CONN_ENGDB)
    cursor = cnxn.cursor()
    cursor.fast_executemany = True
    start = timer()
    cnxn.timeout = 60
    print("Database Connect Time = " + str(round((timer() - start), 3)) + " sec")
    sql = f"""-- noinspection SqlResolveForFile

            SELECT DISTINCT [{comp_name}]
            FROM production.tblTbolt 
            WHERE  ([{comp_name}] IS NOT NULL)
            ORDER BY [{comp_name}]
        """
    try:
        start = timer()
        cursor.execute(sql)
        result = cursor.fetchall()
    except pyodbc.Error as e:
        msg = f'Tbolt {comp_name} List Query Failed: ' + str(e) + " [" + sys._getframe(0).f_code.co_name + "]"
        logger.error(msg)
        print(msg)
        sys.exit(1)
        #common_funcs.send_text(msg)
    else:
        msg = str(len(result)) + f" Unique {comp_name} Part Numbers Retrieved in " \
              + str(round((timer() - start), 3)) + " sec."
        print(msg)

    dbase = []
    for row in result:
        dbase.append(list(row))
    cnxn.close()
    return dbase


def update_compdb(dbase, tbl_name, comp_name):
    """ Add part list to SQL server database"""
    print('Clearing Data Table and Adding Unique Part Numbers From Product Table')
    dbcnxn = pyodbc.connect(CONN_ENGDB)
    cursor = dbcnxn.cursor()
    cursor.fast_executemany = True
    start = timer()
    try:
        cursor.execute(f"TRUNCATE TABLE {tbl_name}")
        dbcnxn.commit()
    except pyodbc.Error as e:
        msg = 'MSSQL Table Deletion Failed: ' + str(e) + " [" + sys._getframe(0).f_code.co_name + "]"
        logger.error(msg)
        # common_funcs.send_text(msg)
        sys.exit(1)
    else:
        print("Delete Time = " + str(round((timer() - start), 3)) + " sec")

    # Load list of part numbers onto SQL server
    sql = f"INSERT INTO {tbl_name} ([{comp_name}]) VALUES (?)"
    try:
        start = timer()
        cursor.executemany(sql, dbase)
        dbcnxn.commit()
    except pyodbc.Error as e:
        msg = 'MSSQL Table Update Failed: ' + str(e) + " [" + sys._getframe(0).f_code.co_name + "]"
        logger.error(msg)
        print(msg)
        # common_funcs.send_text(msg)
        sys.exit(1)
    else:
        print(str(len(dbase)) + f" {comp_name} Part Numbers Added in " + str(round((timer() - start), 3)) + " sec")
    dbcnxn.close()
    return dbase


def build_data_tbl():
    """Build master Tbolt Data Table"""
    dbcnxn = pyodbc.connect(CONN_ENGDB)
    cursor = dbcnxn.cursor()
    if checktblexists(dbcnxn, 'tblTbolt'):
        cursor.execute('DROP TABLE [AutoData].[production].[tblTbolt]')
    try:
        cursor.execute(buildtbl_sql())
        dbcnxn.commit()
    except pyodbc.Error as e:
        msg = 'Tbolt Table Build Failed: ' + str(e) + " [" + sys._getframe(0).f_code.co_name + "]"
        logger.error(msg)
        print(msg)
        # common_funcs.send_text(msg)
        sys.exit(1)
    dbcnxn.close()
    return


def update_db(dbase):
    """ Add part list to SQL server database"""
    print('Clearing Data Table and Adding Unique Part Numbers From Product Table')
    dbcnxn = pyodbc.connect(CONN_ENGDB)
    cursor = dbcnxn.cursor()
    cursor.fast_executemany = True

    # Load list of part numbers onto SQL server
    sql = """INSERT INTO production.tblTbolt (PART) VALUES (?)"""
    try:
        start = timer()
        cursor.executemany(sql, dbase)
        dbcnxn.commit()
    except pyodbc.Error as e:
        msg = 'MSSQL Table Update Failed: ' + str(e) + " [" + sys._getframe(0).f_code.co_name + "]"
        logger.error(msg)
        print(msg)
        # common_funcs.send_text(msg)
        sys.exit(1)
    else:
        print(str(len(dbase)) + " Part Numbers Added in " + str(round((timer() - start), 3)) + " sec")
    dbcnxn.close()
    return dbase


def buildtype_sql():
    """Generate SQL String Based on Component Type"""
    types = get_types()
    sql = """
            SELECT DISTINCT tcTBiD
            FROM NewIdealCurrentSQL.dbo.tblTBComponents 
            WHERE  (tcCat IS NOT NULL)\n"""
    x = ''
    for item in types:
        x = x + f'              OR (tcCat = \'{item}\'' + ')\n'
    strsql = sql + x + '           ORDER BY tcTBiD'
    return strsql


def buildtbl_sql():
    """Generate SQL String To Create Table"""
    types = get_types()
    sql = """
            CREATE TABLE [AutoData].[production].[tblTbolt] (
                   [PART] NVARCHAR (50) NOT NULL,
                   """
    x = ''
    for item in types:
        x += f"""[{item}] NVARCHAR (50) NULL,
                   [{item}_qty] FLOAT (53) DEFAULT ((0)) NULL,
                   """

    strsql = sql + x + 'CONSTRAINT [PK_tblTbolt] PRIMARY KEY CLUSTERED ([PART] ASC));'
    return strsql


def get_partlist():
    """Get Current part list from Prod Eng database."""
    print('Getting Unique Part Numbers From Product Table')
    cnxn = pyodbc.connect(CONN_PRODDB)
    cursor = cnxn.cursor()
    cursor.fast_executemany = True
    start = timer()
    cnxn.timeout = 60
    print("Database Connect Time = " + str(round((timer() - start), 3)) + " sec")
    sql = buildtype_sql()
    try:
        start = timer()
        cursor.execute(sql)
        result = cursor.fetchall()
    except pyodbc.Error as e:
        msg = 'Tbolt Part List Query Failed: ' + str(e) + " [" + sys._getframe(0).f_code.co_name + "]"
        logger.error(msg)
        print(msg)
        # common_funcs.send_text(msg)
        sys.exit(1)
    else:
        msg = str(len(result)) + " Unique Tbolt Part Numbers Retrieved in " + str(round((timer() - start), 3)) + " sec."
        print(msg)
    dbase = []
    for row in result:
        dbase.append(list(row))
    cnxn.close()
    return dbase


def get_tbolt():
    """Get Current part data file from Prod Eng database."""
    print('Getting All Tbolt Records From Product Table')
    cnxn = pyodbc.connect(CONN_PRODDB)
    cursor = cnxn.cursor()
    cursor.fast_executemany = True
    start = timer()
    cnxn.timeout = 60
    print("Database Connect Time = " + str(round((timer() - start), 3)) + " sec")
    types = get_types()
    dbase = []
    for item in types:
        sql = f"""
            SELECT tcTBiD,tcProdid,tcCat,tcQty
            FROM NewIdealCurrentSQL.dbo.tblTBComponents 
            WHERE tcCat = \'{item}\'
            """
        try:
            start = timer()
            cursor.execute(sql)
            result = cursor.fetchall()
        except pyodbc.Error as e:
            msg = 'Tbolt Query Failed: ' + str(e) + " [" + sys._getframe(0).f_code.co_name + "]"
            logger.error(msg)
            print(msg)
            # common_funcs.send_text(msg)
            sys.exit(1)
        else:
            msg = str(len(result)) + " " + item + "s Retrieved in " + str(round((timer() - start), 3)) + " sec."
            print(msg)
        for row in result:
            dbase.append(list([str(x) for x in row]))
    cnxn.close()
    return dbase


def get_types():
    """Get component type data from Prod Eng database."""
    print('Getting Type assignments From Product Table')
    cnxn = pyodbc.connect(CONN_PRODDB)
    cursor = cnxn.cursor()
    cursor.fast_executemany = True
    start = timer()
    cnxn.timeout = 60
    print("Database Connect Time = " + str(round((timer() - start), 3)) + " sec")
    sql = """
            SELECT DISTINCT tcCat
            FROM NewIdealCurrentSQL.dbo.tblTBComponents 
            WHERE  (tcCat IS NOT NULL)
            AND (tcCat <> 'Material')
            ORDER BY tcCat
        """
    try:
        start = timer()
        cursor.execute(sql)
        result = cursor.fetchall()
    except pyodbc.Error as e:
        msg = 'Component Type Query Failed: ' + str(e) + " [" + sys._getframe(0).f_code.co_name + "]"
        logger.error(msg)
        print(msg)
        # common_funcs.send_text(msg)
        sys.exit(1)
    else:
        msg = str(len(result)) + " Types Retrieved in " + str(round((timer() - start), 3)) + " sec."
        print(msg)
    types = []
    for row in result:
        types.append(row[0])
    cnxn.close()
    return types


def build_data(dbase):
    """Build data set and update table"""
    print('Rebuilding Data Table')
    start = timer()
    cnxn = pyodbc.connect(CONN_ENGDB)
    cursor = cnxn.cursor()
    cursor.fast_executemany = True
    for item in dbase:
        sql = f'''
                UPDATE production.tblTbolt 
                SET [{item[2]}] = \'{item[1]}\', [{item[2]}_qty] = \'{item[3]}\'
                WHERE PART = \'{item[0]}\'
                '''
        try:
            cursor.execute(sql)
        except pyodbc.Error as e:
            msg = 'Tbolt Table Build Failed: ' + str(e) + " [" + sys._getframe(0).f_code.co_name + "]"
            logger.error(msg)
            print(msg)
            # common_funcs.send_text(msg)
            sys.exit(1)
    cnxn.commit()
    cnxn.close()
    print(str(len(dbase)) + " Records Processed in " + str(round((timer() - start), 3)) + " sec")
    return


def checktblexists(dbcon, tablename):
    """Check if Database Table Exists"""
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True
    dbcur.close()
    return False


def main():
    """Main Function."""
    #comp = 'Band'
    #update_compdb(get_complist(comp), 'production.tblTboltUnivee', comp)

    build_data_tbl()
    update_db(get_partlist())
    build_data(get_tbolt())
    return


if __name__ == '__main__':
    main()
