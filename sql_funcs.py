import pyodbc
import os
from urllib import parse

# Constants
TRUNCATE_EMPLOYEE_TABLE = "TRUNCATE TABLE production.EMPLOYEE"
TRUNCATE_TBLUSAGE = "TRUNCATE TABLE dbo.tblUsage"
TRUNCATE_TBLINVENTORY = "TRUNCATE TABLE dbo.tblInventory"

INSERT_EMPLOYEE = """INSERT INTO production.EMPLOYEE (ID, NAME, ROLE) VALUES (?, ?, ?)"""
INSERT_USAGE = """INSERT INTO dbo.tblUsage (Date, Part, EngPart, Dept, Acct, Clock, Machine, Qty, Cost, SubTotal) 
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
INSERT_INVENTORY = """INSERT INTO dbo.tblInventory (PartNum, EngPartNum, Desc1, Desc2, Mfg, MfgPn, Cabinet, Drawer, 
                    OnHand, StandardCost, ReOrderDate, DeptUse, DeptPurch, ReOrderPt) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

# Database connection settings
CONNECTION_STRING = (
    f"Driver={{SQL Server}};"
    f"Server=tn-sql;"
    f"Database=autodata;"
    f"UID={os.getenv('SQL_UID')};"
    f"PWD={os.getenv('SQL_PWD')};"
)


def execute_query(query: str, params: list = None):
    """
    Execute a generic SQL query within a context manager to automatically handle connections.
    Args:
        query: The SQL query as a string.
        params: Optional parameters for parameterized queries.
    """
    with pyodbc.connect(CONNECTION_STRING) as connection:
        with connection.cursor() as cursor:
            cursor.fast_executemany = True
            if params:
                cursor.executemany(query, params)
            else:
                cursor.execute(query)
            connection.commit()


def update_database(data: list, truncate_query: str, insert_query: str):
    """
    Generic function to handle updating databases - truncating and inserting data.
    Args:
        data: The data to insert.
        truncate_query: The SQL query to truncate the target table.
        insert_query: The SQL query to insert data into the target table.
    """
    print("Deleting existing records on SQL Server...")
    execute_query(truncate_query)
    print("Loading data to SQL Server...")
    try:
        execute_query(insert_query, data)
        print(f"{len(data)} Records Processed")
    except Exception as e:
        print(f"SQL Query Failed: {e}")


def update_dbusage(data: list):
    """Update Spare Part Usage Data."""
    update_database(data, TRUNCATE_TBLUSAGE, INSERT_USAGE)


def update_emps(data: list):
    """Update Employee Data."""
    update_database(data, TRUNCATE_EMPLOYEE_TABLE, INSERT_EMPLOYEE)


def update_dbinv(data: list):
    """Update Spare Part Inventory Data."""
    update_database(data, TRUNCATE_TBLINVENTORY, INSERT_INVENTORY)
