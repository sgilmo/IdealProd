"""Refresh SQL Server employee data and publish the machine login CSV."""

import csv
import logging
from pathlib import Path
from tempfile import NamedTemporaryFile

import as400
import config
import sql_funcs

logger = logging.getLogger(__name__)
NO_LOGIN_USER = ('0000', 'Please Log In', 'DEF')
CSV_QUOTE_NONE = csv.QUOTE_NONE


def main() -> None:
    """Refresh employee data, then rebuild the scrap table."""
    logger.info("Fetching employee records from AS400")
    employee_data = as400.get_emps()
    logger.info("Fetched %d employee rows including the engineering login", len(employee_data))

    output_path: Path = config.FTP_ROOT / 'emps.csv'
    staged_path = None
    try:
        logger.info("Staging employee CSV in %s", output_path.parent)
        with NamedTemporaryFile(
            mode='w', newline='', dir=output_path.parent, suffix='.tmp', delete=False
        ) as output_file:
            staged_path = Path(output_file.name)
            writer = csv.writer(output_file, quoting=CSV_QUOTE_NONE, escapechar='\\')
            writer.writerows(employee_data)
            writer.writerow(NO_LOGIN_USER)

        logger.info("Refreshing SQL Server employee table")
        sql_funcs.update_emps(employee_data)
        logger.info("SQL Server employee refresh committed")

        try:
            staged_path.replace(output_path)
        except OSError:
            logger.error("SQL committed but publishing %s failed", output_path)
            raise
        logger.info("Published %s", output_path)
    finally:
        if staged_path is not None:
            try:
                staged_path.unlink(missing_ok=True)
            except OSError:
                logger.warning("Could not remove staged CSV %s", staged_path, exc_info=True)

    logger.info("Rebuilding scrap table")
    as400.build_scrap_table()
    logger.info("Scrap table rebuild returned")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
