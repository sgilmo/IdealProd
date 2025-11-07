import pyodbc
import os
from urllib import parse
from sqlalchemy import create_engine
from sqlalchemy import text

# Constants
TRUNCATE_EMPLOYEE_TABLE = "TRUNCATE TABLE production.EMPLOYEE"
TRUNCATE_TBLUSAGE = "TRUNCATE TABLE dbo.tblUsage_temp"
TRUNCATE_TBLPROD = "TRUNCATE TABLE eng.tblProd_temp"
TRUNCATE_TBLINVENTORY = "TRUNCATE TABLE dbo.tblInventory"
TRUNCATE_TBLORDERS = "TRUNCATE TABLE dbo.tblOrders"
TRUNCATE_TBLALLORDERS = "TRUNCATE TABLE dbo.tblOrdersAll"

INSERT_EMPLOYEE = """INSERT INTO production.EMPLOYEE (ID, NAME, ROLE) VALUES (?, ?, ?)"""
INSERT_USAGE = """INSERT INTO dbo.tblUsage_temp (Date, Part, EngPart, Dept, Acct, Clock, Machine, Qty, Cost, SubTotal) 
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
INSERT_INVENTORY = """INSERT INTO dbo.tblInventory (PartNum, EngPartNum, Desc1, Desc2, Mfg, MfgPn, Cabinet, Drawer, 
                    OnHand, StandardCost, ReOrderDate, DeptUse, DeptPurch, ReOrderPt) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
INSERT_PROD = """INSERT INTO eng.tblProd_temp (IDEB_WEEK, IDEB_DAY, IDEB_DEPT, IDEB_EMP_NBR, IDEB_SHIFT, IDEB_MACH_NBR,
                    IDEB_PART, IDEB_TICKET_NBR, IDEB_TOTAL_QTY, IDEB_STANDARD, IDEB_ACTUAL_HOURS, IDEB_OVERTIME_HOURS, 
                    IDEB_MONTH) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
INSERT_ORDERS = """INSERT INTO dbo.tblOrders (OrderNum, PartNumber, Qty, Machine) VALUES (?, ?, ?, LEFT(?,3))"""
INSERT_ALL_ORDERS = """INSERT INTO dbo.tblOrdersAll (OrderNum, PartNumber, Qty, Note) VALUES (?, ?, ?, ?)"""

# Database connection settings
CONNECTION_STRING = (
    f"Driver={{SQL Server}};"
    f"Server=tn-sql;"
    f"Database=autodata;"
    f"UID={os.getenv('SQL_UID')};"
    f"PWD={os.getenv('SQL_PWD')};"
)

# SQLAlchemy connection
server = 'tn-sql'
database = 'autodata'
driver = 'ODBC+Driver+17+for+SQL+Server'
user = os.getenv('SQL_UID')
pwd = parse.quote_plus(os.getenv('SQL_PWD'))
port = '1433'
database_conn = f'mssql+pyodbc://{user}:{pwd}@{server}:{port}/{database}?driver={driver}'
# Make Connection
engine = create_engine(database_conn)


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
    print("Updating Spare Part Usage Data...")
    update_database(data, TRUNCATE_TBLUSAGE, INSERT_USAGE)

def update_dbprod(data: list):
    """Update Spare Part Usage Data."""
    print("Updating Spare Part Production Data...")
    update_database(data, TRUNCATE_TBLPROD, INSERT_PROD)


def update_emps(data: list):
    """Update Employee Data."""
    print("Updating Employee Data...")
    update_database(data, TRUNCATE_EMPLOYEE_TABLE, INSERT_EMPLOYEE)


def update_dbinv(data: list):
    """Update Spare Part Inventory Data."""
    print("Updating Spare Part Inventory Data...")
    update_database(data, TRUNCATE_TBLINVENTORY, INSERT_INVENTORY)

def update_orders(data: list):
    """Update Spare Part Inventory Data."""
    update_database(data, TRUNCATE_TBLORDERS, INSERT_ORDERS)

def update_all_orders(data: list):
    """Update Spare Part Inventory Data."""
    update_database(data, TRUNCATE_TBLALLORDERS, INSERT_ALL_ORDERS)


def sync_usage(schema="dbo", src_table="tblUsage_temp", dst_table="tblUsage"):
    """
    Insert rows from schema.src_table into schema.dst_table that do not already exist in dst_table.
    - Excludes Id, identity, computed, and rowversion/timestamp columns.
    - Uses null-safe equality in NOT EXISTS to avoid duplicates with NULLs.
    - Runs as a single set-based statement for performance.
    Requires a configured SQLAlchemy engine.
    """
    print(f"Syncing {schema}.{src_table} into {schema}.{dst_table}...")
    with engine.begin() as conn:
        # 1) Discover common, insertable columns in source and destination
        cols_sql = text("""
            WITH cols AS (
                SELECT
                    s.name  AS schema_name,
                    t.name  AS table_name,
                    c.name  AS column_name,
                    c.column_id,
                    c.is_identity,
                    c.is_computed,
                    ty.name AS type_name
                FROM sys.schemas s
                JOIN sys.tables t   ON t.schema_id = s.schema_id
                JOIN sys.columns c  ON c.object_id = t.object_id
                JOIN sys.types ty   ON ty.user_type_id = c.user_type_id
                WHERE s.name = :schema
                  AND t.name IN (:src, :dst)
            )
            SELECT d.column_name
            FROM cols d
            JOIN cols s
              ON s.table_name = :src
             AND d.table_name = :dst
             AND s.schema_name = d.schema_name
             AND s.column_name = d.column_name
            WHERE d.schema_name = :schema
              AND d.is_identity = 0
              AND d.is_computed = 0
              AND d.type_name NOT IN ('timestamp', 'rowversion')
              AND d.column_name <> 'Id'
            ORDER BY d.column_id
        """)
        insert_cols = list(conn.execute(
            cols_sql,
            {"schema": schema, "src": src_table, "dst": dst_table}
        ).scalars())

        if not insert_cols:
            print("No insertable common columns found.")
            return

        # 2) Build the INSERT...SELECT...WHERE NOT EXISTS with NULL-safe comparisons
        col_list = ", ".join(f"[{c}]" for c in insert_cols)
        # d.[c] = t.[c] OR (d.[c] IS NULL AND t.[c] IS NULL)
        where_parts = [f"(d.[{c}] = t.[{c}] OR (d.[{c}] IS NULL AND t.[{c}] IS NULL))"
                       for c in insert_cols]
        where_clause = " AND ".join(where_parts)

        sql = f"""
        INSERT INTO [{schema}].[{dst_table}] ({col_list})
        SELECT {col_list}
        FROM   [{schema}].[{src_table}] t
        WHERE NOT EXISTS (
            SELECT 1
            FROM [{schema}].[{dst_table}] d WITH (HOLDLOCK, UPDLOCK)
            WHERE {where_clause}
        )
        """

        conn.exec_driver_sql(sql)
        print(f"Sync complete: inserted new rows from {schema}.{src_table} into {schema}.{dst_table}.")

