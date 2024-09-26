"""Functions Required for Engineering Spare Part Requisitions"""
import sqlserver
import common_funcs
import pandas as pd

def build_email():
    """ Build Email Struct and send to appropriate people"""
    mailto = ["sgilmour@idealtridon.com", "bbrackman@idealtridon.com", "jmoore@idealtridon.com",
              "rjobman@idealtridon.com"]
    # mailto = ["sgilmour@idealtridon.com"]
    item_list = []
    i = 1
    reqspare = sqlserver.get_spare_req()
    if len(reqspare) > 0:
        for item in reqspare:
            item_list.append('<h5>Item ' + str(i) + '</h5>'
                             + '<p style="margin-left: 40px">'
                             + 'Description: <strong>' + item.desc + '</strong>'
                             + '<br>Requested By: ' + item.req_by
                             + '<br>Manufacturer: ' + item.mfg
                             + '<br>Manufacturer Pn: <strong>' + item.mfg_pn + '</strong>'
                             + '<br>Drawing: ' + item.dwg
                             + '<br>Vendor: ' + item.vendor
                             + ', Cost: $' + str(common_funcs.set_precision(item.cost, 2))
                             + ', Unit: ' + item.unit
                             + '<br><br>Initial Order: ' + str(item.qty_stock)
                             + ', Qty Per Use: ' + str(item.qty_per_use)
                             + ', Annual Usage: ' + str(item.qty_annual_use)
                             + '<br>Dept Usage: ' + item.depts
                             + '<br>Reorder Pt: ' + str(item.reorder_pt)
                             + ', Reorder Amt: ' + str(item.reorder_amt)
                             + '</p><br>')
            i += 1
        stritems = ""
        if i > 2:
            stritems = " Items Available in the Tool Crib</h3>"
        if i <= 2:
            stritems = " Item Available in the Tool Crib</h3>"
        common_funcs.build_email(item_list, "Spare Part Request",
                                 "<br><h3>Please Make the Following " + str(i-1) + stritems, mailto)
    return


def build_email_obs(obs_spare):
    """ Build Email Struct and send to appropriate people"""
    item_list = []
    i = 1
    # mailto = ["sgilmour@idealtridon.com", "bbrackman@idealtridon.com"]
    mailto = ["sgilmour@idealtridon.com"]
    # obs_spare = sqlserver.find_new_obs()
    if len(obs_spare) > 0:
        for item in obs_spare:
            item_list.append('<h5>Item ' + str(i) + '</h5>'
                             + '<p style="margin-left: 40px">'
                             + 'Part Number: ' + str(item[1])
                             + '<br>Eng Part Number: <strong>' + str(item[2]) + '</strong>'
                             + '<br>Description 1: ' + str(item[3])
                             + '<br>Description 2: ' + str(item[4])
                             + '<br>Manufacturer: ' + str(item[5])
                             + '<br>Manufacturer Pn: <strong>' + str(item[6]) + '</strong>'
                             + '<br>Cabinet: ' + str(item[7])
                             + '<br>Draw: ' + str(item[8])
                             + '<br>On Hand: ' + str(item[9])
                             + '<br>Cost: $' + str(item[10])
                             + '<br>Date: $' + str(item[11])
                             + '<br>Dept Use: ' + str(item[12])
                             + '<br>Dept Purch: ' + str(item[13])
                             + '</p><br>')
            i += 1
        stritems = ""
        if i > 2:
            stritems = str(i-1) + " Items Have Been Made Obsolete</h3>"
        if i <= 2:
            stritems = "Item Has Been Made Obsolete</h3>"
        common_funcs.build_email(item_list, "Newly Obsoleted Spare Parts", "<br><h3>The Following "
                                 + stritems, mailto)

    return


def build_email_obs2(obs_spare):
    """ Build Email Struct and send to appropriate people"""
    mailto = ["sgilmour@idealtridon.com", "bbrackman@idealtridon.com"]
    stritems = "Item(s) Have Been Made Obsolete</h3>"
    common_funcs.build_email(obs_spare, "Newly Obsoleted Spare Parts", "<br><h3>The Following "
                             + stritems, mailto)
    return


