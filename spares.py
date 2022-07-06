#!/usr/bin/env python

"""Get spare part data from AS400 and place in apprpriate SQL Server tables."""

import as400
import sqlserver


def main():
    """Main Function"""
    sqlserver.update_dbinv(as400.get_inv())
    sqlserver.update_dbusage(as400.get_usage())
    sqlserver.update_emps(as400.get_emps())
    sqlserver.cleanup_pending()
    return


if __name__ == '__main__':
    main()
