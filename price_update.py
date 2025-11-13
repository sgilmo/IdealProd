
"""Update Automation Direct Price Sheet.

Pull Current Pricelist from Automation Direct Website and
Load it on to existing SQL Server Table.
"""
import io
import logging
from timeit import default_timer as timer
from typing import Dict, Optional
from urllib import parse
import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
PRICE_LIST_URL = "https://cdn.automationdirect.com/static/prices/prices_public.xlsx"
SHEET_NAME = "ADC Price List with Categories "
OUTPUT_TABLE = "Adirect"
OUTPUT_SCHEMA = "production"

# Database connection constants
DB_SERVER = 'tn-sql'
DB_NAME = 'autodata'
DB_DRIVER = 'ODBC+Driver+17+for+SQL+Server'
DB_USER = 'production'
DB_PASSWORD = parse.quote_plus("Auto@matics")
DB_PORT = '1433'


def get_db_connection() -> Engine:
    """Create and return a database connection engine.

    Returns:
        Engine: SQLAlchemy database connection engine
    """
    connection_string = (
        f'mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}:{DB_PORT}/{DB_NAME}'
        f'?driver={DB_DRIVER}'
    )
    return create_engine(connection_string)


def read_pricelist() -> Optional[pd.DataFrame]:
    """Fetch and process price list from Automation Direct website.

    Returns:
        Optional[pd.DataFrame]: Processed price list dataframe or None if fetching fails
    """
    logger.info('Fetching data file from Automation Direct website')

    # Create browser-like headers
    headers: Dict[str, str] = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/vnd.ms-excel',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.automationdirect.com/'
    }

    try:
        # Make the request with appropriate headers
        response = requests.get(PRICE_LIST_URL, headers=headers, verify=True)
        response.raise_for_status()

        # Read the Excel data
        excel_data = io.BytesIO(response.content)
        df = pd.read_excel(excel_data, sheet_name=SHEET_NAME, usecols=[0, 2, 3])

        # Process dataframe
        df = df.dropna().reset_index(drop=True)
        df.columns = ['part', 'status', 'price']
        df = df[1:]  # Skip the first row

        return df

    except requests.RequestException as e:
        logger.error(f"Failed to download price list file: {e}")
        return None
    except Exception as e:
        logger.error(f"Error processing price list data: {e}")
        return None


def update_db(df: pd.DataFrame, engine: Engine) -> bool:
    """Update database with price list data.

    Args:
        df: Dataframe containing price list data
        engine: SQLAlchemy database engine

    Returns:
        bool: True if update successful, False otherwise
    """
    if df is None or df.empty:
        logger.error("No data to update database")
        return False

    logger.info(f"Updating database with {len(df)} price records")
    try:
        df.to_sql(
            OUTPUT_TABLE,
            engine,
            schema=OUTPUT_SCHEMA,
            if_exists='replace',
            index=False
        )
        return True
    except SQLAlchemyError as e:
        logger.error(f"Database update failed: {e}")
        return False


def main() -> None:
    """Main execution function."""
    start = timer()

    try:
        # Get database connection
        engine = get_db_connection()

        # Fetch and process price list
        price_data = read_pricelist()
        if price_data is None:
            logger.error("Failed to retrieve price data. Exiting.")
            return

        # Update database
        success = update_db(price_data, engine)

        # Log result
        if success:
            elapsed_time = timer() - start
            logger.info(f"Price update completed successfully in {round(elapsed_time, 3)} seconds")
        else:
            logger.error("Price update failed")

    except Exception as e:
        logger.error(f"Unexpected error in price update process: {e}")
    finally:
        logger.info(f"Total execution time: {round(timer() - start, 3)} sec")


if __name__ == '__main__':
    main()
