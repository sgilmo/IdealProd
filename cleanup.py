# coding=utf-8
# !/usr/bin/env python

"""This program deletes ACM camera files older than 10 days on the engineering server"""

import os
import time
import logging
from timeit import default_timer as timer

# Setup Logging
# Gets or creates a logger
logger = logging.getLogger()

# set log level
logger.setLevel(logging.INFO)

# define file handler and set formatter
file_handler = logging.FileHandler('c:\\logs\\cleanup.log')
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
file_handler.setFormatter(formatter)

# add file handler to logger
logger.addHandler(file_handler)

FILES_REMOVED = 0
FOLDERS_REMOVED = 0


def remove(path):
    """
    Remove the file or directory
    """
    global FOLDERS_REMOVED
    global FILES_REMOVED
    if os.path.isdir(path):
        try:
            os.rmdir(path)
            FOLDERS_REMOVED += 1
        except OSError:
            msg = "Unable to remove folder: %s" % path
            logger.error(msg + " [" + remove.__name__ + "]")
    else:
        try:
            if os.path.exists(path):
                os.remove(path)
                FILES_REMOVED += 1
        except OSError:
            msg = "Unable to remove file: %s" % path
            logger.error(msg + " [" + remove.__name__ + "]")
    return


def cleanup(number_of_days, path):
    """
    Removes files from the passed in path that are older than or equal
    to the number_of_days
    """

    filecnt = 0
    time_in_secs = time.time() - (number_of_days * 24 * 60 * 60)
    for root, dirs, files in os.walk(path, topdown=False):
        for file_ in files:
            full_path = os.path.join(root, file_)
            print(full_path)
            stat = os.stat(full_path)
            filecnt += 1
            if stat.st_mtime <= time_in_secs:
                msg = "Removing: %s" % file_
                logger.info(msg)
                print(msg)
                remove(full_path)

        if not os.listdir(root):
            msg = "Removing Empty Folder: %s" % root
            logger.info(msg)
            print(msg)
            remove(root)
    msg = "Files Scanned = " + str(filecnt)
    logger.info(msg)
    return


def main():
    """Main Function"""
    number_of_days = 10
    root = ('\\\\tn-san-fileserv\\TOOLING\\2794 ACM Screw Head Vision\\Camera\\Screw Images\\Bad\\',
            '\\\\tn-san-fileserv\\TOOLING\\2874 PREFORM CLAMP AUTOMATION\\Vision\\Lok Images\\Bad\\',
            '\\\\tn-san-fileserv\\TOOLING\\2874 PREFORM CLAMP AUTOMATION\\Vision\\Lok Images\\Good\\')
    start = timer()
    logger.info("Program Started")
    for item in root:
        print("Checking: " + item)
        cleanup(number_of_days, item)
    msg = "Files Deleted = " + str(FILES_REMOVED)
    logger.info(msg)
    msg = "Folders Deleted = " + str(FOLDERS_REMOVED)
    logger.info(msg)
    logger.info("File Cleanup Complete in " + str(round((timer() - start), 3)) + " sec")
    return


if __name__ == '__main__':
    main()
