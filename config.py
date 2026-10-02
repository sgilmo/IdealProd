# config.py
"""Centralized configuration. Values come from environment variables, with defaults for non-secrets."""
import os
from pathlib import Path


def _require(name):
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f'{name} environment variable must be set')
    return value


def _list(name, default):
    return [s.strip() for s in os.getenv(name, default).split(',') if s.strip()]


# Database
SQL_SERVER = os.getenv('SQL_SERVER', 'tn-sql')
SQL_DATABASE = os.getenv('SQL_DATABASE', 'autodata')
SQL_PORT = os.getenv('SQL_PORT', '1433')
SQL_DRIVER = os.getenv('SQL_DRIVER', 'ODBC Driver 17 for SQL Server')
SQL_UID = _require('SQL_UID')
SQL_PWD = _require('SQL_PWD')

# Paths
APP_DIR = Path(__file__).resolve().parent
LOG_FILE = Path(os.getenv('IDEALPROD_LOG_FILE', APP_DIR / 'update.log'))
FTP_ROOT = Path(os.getenv('FTP_ROOT', r'C:\Inetpub\ftproot'))
ACM_BAD_SCREWS_PATH = Path(os.getenv('ACM_BAD_SCREWS_PATH', r'\\tn-file02\tooling\2794 ACM Screw Head Vision\Camera\Screw Images\Bad'))
FASTLOK_IMAGES_PATH = Path(os.getenv('FASTLOK_IMAGES_PATH', r'\\tn-file02\tooling\2874 PREFORM CLAMP AUTOMATION\Vision\Lok Images'))

# Email
MAILTO = _list('IDEALPROD_MAILTO', 'sgilmour@idealtridon.com')
SQL_ALERT_RECIPIENTS = _list('SQL_ALERT_RECIPIENTS', 'elab@idealtridon.com')