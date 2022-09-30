# coding=utf-8
"""Monitor Directory for Maintenance Request"""
# !/usr/bin/env python

import os
import time
import ftplib
import common_funcs

# Establish Some Globals

PATH_TO_SCAN = "c:\\InetPub\\ftproot\\Monitor\\"


def send_files(filename, ip):
    """Send files to HelpDesk and Maintenance HMI."""
    maint_path = "\\wo\\"
    try:
        print("Transferring " + filename + " To " + ip)
        s = ftplib.FTP(ip, 'anonymous', 'anonymous')
        f = open(PATH_TO_SCAN + filename, "rb")
        s.storbinary('STOR ' + maint_path + filename, f)
        f.close()
        s.quit()
        print("Success")
        return 1
    except Exception as e:
        msg = 'Maint HMI Update Failed: ' + filename + ': ' + str(e)
        common_funcs.send_text(msg)
        print(msg)
        return 0


def main():
    """ Main Function"""
    ip_maint = "10.143.50.99"
    ip_helpdesk = "10.143.12.194"
    before = dict([(f, None) for f in os.listdir(PATH_TO_SCAN)])
    while 1:
        time.sleep(10)
        after = dict([(f, None) for f in os.listdir(PATH_TO_SCAN)])
        added = [f for f in after if f not in before]
        removed = [f for f in before if f not in after]
        if added:
            print("Added: ", ", ".join(added))
            for f1 in added:
                print(PATH_TO_SCAN + f1)
                if send_files(f1, ip_maint) + send_files(f1, ip_helpdesk) == 2:
                    os.remove(PATH_TO_SCAN + f1)
        if removed:
            print("Removed: ", ", ".join(removed))
        before = after


if __name__ == '__main__':
    main()
