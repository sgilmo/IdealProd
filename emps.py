# !/usr/bin/env python
"""
Get employee information from AS400, update SQL Server database with that data,
and create a CSV file in the FTP root directory.
"""
import csv
from typing import List
import as400
import sql_funcs

# Constants
OUTPUT_PATH = "\\Inetpub\\ftproot\\"
OUTPUT_FILENAME = "emps.csv"
NO_LOGIN_USER = ['0000', 'Please Log In', 'DEF']


def fetch_employee_data() -> List[List]:
    """
    Fetch employee data from AS400 and convert tuples to lists.

    Returns:
        List[List]: List of employee data records
    """
    emp_tuple = as400.get_emps()
    return [list(item) for item in emp_tuple]


def update_sql_database(employee_data: List[List]) -> None:
    """
    Update SQL Server database with employee data.

    Args:
        employee_data: List of employee records
    """
    sql_funcs.update_emps(employee_data)


def create_csv_file(employee_data: List[List], filepath: str) -> None:
    """
    Create a CSV file with employee data.

    Args:
        employee_data: List of employee records
        filepath: Path where CSV file will be saved
    """
    # Add no-login user to the data for export
    data_for_export = employee_data.copy()
    data_for_export.append(NO_LOGIN_USER)

    with open(filepath, "w", newline='') as output_file:
        csv_writer = csv.writer(
            output_file,
            delimiter=',',
            quoting=csv.QUOTE_NONE,
            escapechar='\\'
        )
        csv_writer.writerows(data_for_export)


def main() -> None:
    """
    Main function to orchestrate the employee data processing.
    """
    # Get employee data from AS400
    employee_data = fetch_employee_data()


    # Update SQL Server database
    update_sql_database(employee_data)

    # Create CSV file for export to machines
    output_path = f"{OUTPUT_PATH}{OUTPUT_FILENAME}"
    create_csv_file(employee_data, output_path)


if __name__ == "__main__":
    main()