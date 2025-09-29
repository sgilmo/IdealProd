#!/usr/bin/env python

"""Get spare part data from AS400 and place in appropriate SQL Server tables.
Also, process engineering requested spare parts and track them through the system"""

import as400
import sqlserver
import reqspare
import sql_funcs


def main():
    """Main Function"""
    as400_dbresult = as400.get_inv()
    sql_funcs.update_dbinv(as400.build_inv_list(as400_dbresult))
    sqlserver.check_obs(as400_dbresult)

    # Add only new records to tblUsage
    sql_funcs.update_dbusage(as400.get_usage())
    sql_funcs.sync_usage(schema="dbo", src_table="tblUsage_temp", dst_table="tblUsage")
    # Add only new records to tblProd
    sql_funcs.update_dbprod(as400.get_prod())
    sql_funcs.sync_usage(schema="eng", src_table="tblProd_temp", dst_table="FPMGFILE")

    reqspare.make_req()  # Build and Send email to purchasing
    sqlserver.move_entered_spares()  # Check if part entered on AS400
    sqlserver.move_comp_spares()  # Record stocked requested spare to its own table

    changed = sqlserver.check_reorder_pts()
    if len(changed) > 0:
        sqlserver.log_changed_reorderpts(changed)
        sqlserver.reset_ref_table()
    return


if __name__ == '__main__':
    main()
