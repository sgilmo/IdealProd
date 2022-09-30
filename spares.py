#!/usr/bin/env python

"""Get spare part data from AS400 and place in apprpriate SQL Server tables.
Also, process engineering requested spare parts and track them through the system"""

import as400
import sqlserver
import reqspare


def main():
    """Main Function"""
    sqlserver.update_dbusage(as400.get_usage())
    sqlserver.update_emps(as400.get_emps())
    sqlserver.cleanup_pending()
    reqspare.build_email()  # Build and Send email to purchasing
    sqlserver.update_req()  # Timestamp Request when emailed to purchasing
    sqlserver.move_entered_spares()  # Check if part entered on AS400
    sqlserver.move_comp_spares()  # Record stocked requested spare to its own table
    reqspare.build_email_obs()  # Build and Send email to purchasing
    obs = sqlserver.find_new_obs()
    sqlserver.insert_new_obs(obs)  # Insert newly obsoleted part into the reference table
    sqlserver.save_new_obs(obs)  # Save a record of obsoleted part
    # as400.get_inv()
    return


if __name__ == '__main__':
    main()
