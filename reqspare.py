"""Functions Required for Engineering Spare Part Requisitions"""
import sqlserver
import common_funcs


def build_email():
    """ Build Email Struct and send to appropriate people"""
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
        item_list.append('<br><br>Sincerely,<br>'
                         '<br> The Engineering Overlords and Steve<br><br>')
        item_list.append('<a href="https://www.idealtridon.com/idealtridongroup.html"> '
                         '<img src="https://sgilmo.com/email_logo.png" alt="Ideal Logo"></a>')
        stritems =""
        if i > 2:
            stritems = " Items Available in the Tool Crib</h3>"
        if i <= 2:
            stritems = " Item Available in the Tool Crib</h3>"

        common_funcs.send_sparereq_email(item_list, "Spare Part Request",
                                                    "<br><h3>Please Make the Following " + str(i-1) + stritems)
    return


def build_email_obs():
    """ Build Email Struct and send to appropriate people"""
    item_list = []
    i = 1
    obs_spare = sqlserver.find_new_obs()
    if len(obs_spare) > 0:
        for item in obs_spare:
            item_list.append('<h5>Item ' + str(i) + '</h5>'
                             + '<p style="margin-left: 40px">'
                             + 'Part Number: ' + item[1]
                             + '<br>Eng Part Number: <strong>' + item[2] + '</strong>'
                             + '<br>Description 1: ' + item[3]
                             + '<br>Description 2: ' + item[4]
                             + '<br>Manufacturer: ' + item[5]
                             + '<br>Manufacturer Pn: <strong>' + item[6] + '</strong>'
                             + '<br>Cabinet: ' + item[7]
                             + '<br>Draw: ' + item[8]
                             + '<br>On Hand: ' + str(item[9])
                             + '<br>Cost: $' + str(item[10])
                             + '<br>Dept Use: ' + item[12]
                             + '<br>Dept Purch: ' + item[13]
                             + '</p><br>')
            i += 1
        item_list.append('<br><br>Sincerely,<br>'
                         '<br> The Engineering Overlords and Steve<br><br>')
        item_list.append('<a href="https://www.idealtridon.com/idealtridongroup.html"> '
                         '<img src="https://sgilmo.com/email_logo.png" alt="Ideal Logo"></a>')
        stritems = ""
        if i > 2:
            stritems = str(i-1) + " Items Have Been Made Obsolete</h3>"
        if i <= 2:
            stritems = "Item Has Been Made Obsolete</h3>"

        common_funcs.send_newobs_email(item_list, "Newly Obsoleted Spare Parts", "<br><h3>The Following "
                                       + stritems)
    return


