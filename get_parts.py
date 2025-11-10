import json
import os
import csv
import ftplib
import shutil
from datetime import datetime
import common_funcs
from timeit import default_timer as timer
import logging

# Setup Logging
# Gets or creates a logger
logger = logging.getLogger(__name__)

# set log level
logger.setLevel(logging.INFO)

# define file handler and set formatter
file_handler = logging.FileHandler('c:\\logs\\getclamps.log')
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
file_handler.setFormatter(formatter)

# add file handler to logger
logger.addHandler(file_handler)

FORMAT = '%Y%m%d%H%M%S'



def ftp_file_to_machines(filename, filepath, json_file, username='anonymous', password='anonymous'):
    """FTP a file to list of machines from acm_map.json.

    Args:
        filename: Name of the file to transfer (e.g., 'parts.csv')
        filepath: Path where the file is located
        json_file: Path to acm_map.json file
        username: FTP username (default: 'anonymous')
        password: FTP password (default: 'anonymous')

    Returns:
        List of machines that failed to receive the file
    """
    # Load machine IPs from JSON
    with open(json_file, 'r') as f:
        devices = json.load(f)

    unsuccessful_transfers = []
    successful_transfers = []

    full_path = os.path.join(filepath, filename)

    if not os.path.exists(full_path):
        print(f"Error: File {full_path} does not exist")
        return None

    for machine, ip in devices.items():
        try:
            print(f"Transferring {filename} to {machine} ({ip})...")

            with ftplib.FTP(ip, username, password, timeout=10) as ftp:
                with open(full_path, 'rb') as f:
                    dest = f'STOR /MEMCARD1/{filename}' if machine[-3:] == 'PLC' else f'STOR {filename}'
                    ftp.storbinary(dest, f)

            successful_transfers.append(machine)
            print(f"  ✓ Success: {machine}")

        except Exception as e:
            unsuccessful_transfers.append((machine, ip, str(e)))
            print(f"  ✗ Failed: {machine} - {str(e)}")

    # Print summary
    print(f"\n{'='*60}")
    print(f"Transfer Summary:")
    print(f"  Successful: {len(successful_transfers)}")
    print(f"  Failed: {len(unsuccessful_transfers)}")
    print(f"{'='*60}")

    if unsuccessful_transfers:
        print("\nFailed transfers:")
        for machine, ip, error in unsuccessful_transfers:
            print(f"  {machine} ({ip}): {error}")

    return unsuccessful_transfers

def compare_csv_files(file1, file2):
    """Compare CSV files and return the differences."""

    # Read first file
    with open(file1, 'r') as f1:
        reader1 = csv.reader(f1)
        data1 = [row for row in reader1]

    # Read second file
    with open(file2, 'r') as f2:
        reader2 = csv.reader(f2)
        data2 = [row for row in reader2]

    # Find differences
    differences = []

    # Rows in file1 but not in file2
    for row in data1:
        if row not in data2:
            differences.append(('In parts.csv only', row))

    # Rows in file2 but not in file1
    for row in data2:
        if row not in data1:
            differences.append(('In parts_clamps.csv only', row))

    return differences

def log_changes(chglist, path):
    """Create log file for changes."""
    file_connect = open(path + "upd_" + datetime.now().strftime(FORMAT) + ".log", "w")
    # Add changed parts to file
    file_connect.write('Parts that were updated:\n')
    for location, row in chglist:
        file_connect.write(f"{location}: {row}" + '\n')
    #for part in chglist:
        #file_connect.write(part + '\n')
    file_connect.close()
    return

def check_maint_files(mpath, qpath):
    """Check if Maint added any modified programs to server."""
    print('Checking for Maintenance Files')
    newfiles = []
    filelist = []
    try:
        for fname in os.listdir(mpath):
            if os.path.isfile(os.path.join(mpath, fname)):
                filelist.append(fname)
        for filename in filelist:
            print(filename)
            shutil.copy2(mpath + filename, qpath + filename)
            os.remove(mpath + filename)
            newfiles.append(filename)
        if len(newfiles) > 0:
            print(str(len(newfiles)) + ' Files Found')
            common_funcs.send_email(newfiles, "File(s) Added to MAINT Directory",
                                    "The following file(s) need to be reviewed:\n\n")
            print('Done')
    except Exception as e:
        msg = 'Maint File Sweep Failed: ' + ': ' + str(e) + " [" + check_maint_files.__name__ + "]"
        logger.error(msg)
        print(msg)
    return

def copy_and_rename(source_path, dest_folder, new_name):
    """Copy file to destination with a new name."""
    dest_path = os.path.join(dest_folder, new_name)
    shutil.copy(source_path, dest_path)
    return dest_path

def save_history(filename, partpath, histpath):
    """Move file to history folder with timestamp as new name."""
    shutil.copy2(partpath + filename, histpath +
                 datetime.now().strftime(FORMAT) + ".csv")
    return


def email_bad_machines(machlist):
    """Create a log file of machines that had connection issues."""
    mailto = ["elab@idealtridon.com"]  # storing the receiver's mail id
    # Send the Email
    if len(machlist) > 1:
        subject = "The Following Machines Could Not Recieve New Part Data Files"
        header = "The Following Machines could not<br>be reached via FTP:\n\n"
    else:
        subject = "The Following Machine Could Not Recieve New Part Data Files"
        header = "The Following Machine could not<br>be reached via FTP:\n\n"
    common_funcs.build_email(machlist, subject, header, mailto)
    return

def main():
    """Main Function."""
    # Define file paths
    partdatapath = "\\Inetpub\\ftproot\\acmparts\\"
    parthistpath = "\\Inetpub\\ftproot\\acmparts\\history\\"
    maintpath = "\\\\tn-san-fileserv\\dept\\Engineering\\Engineering Data\\Elab\\Machines\\MAINT\\"
    quepath = "\\\\tn-san-fileserv\\dept\\Engineering\\Engineering Data\\Elab\\Machines\\ReviewQue\\"
    filename = 'test.csv'
    filepath = r'c:\temp'
    json_file = r'C:\Python Projects\IdealProd2\acm_map2.json'
    parthistpath = r'y:\inetpub\ftproot\acmparts\history\\'
    dest_folder = r'y:\inetpub\ftproot\acmparts\\'
    curr_file = r'y:\inetpub\ftproot\acmparts\parts.csv'
    new_file = r'c:\temp\parts_clamps.csv'

    start = timer()

    print("Comparing CSV files...")
    diffs = compare_csv_files(curr_file, new_file)


    if len(diffs) > 0:
        # Log differences
        num_diffs = str(len(diffs))
        print('Part file different (' + num_diffs + '), replacing part file and sending to machines')
        for x in diffs:
            print(x)
        log_changes(diffs, parthistpath)
        msg = "Number of Parts Modified, Added or Deleted = " + num_diffs
        logger.info(msg)
        print(msg)

        # Save Copy
        save_history("parts.csv", dest_folder, parthistpath)
        # Replace file
        copy_and_rename(new_file, dest_folder, 'steve.csv')

        # Send to machines
        failed = ftp_file_to_machines(filename, filepath, json_file)
        if failed:
            email_bad_machines([str(x) for x in failed])
            print("Failed to transfer files to machines:")
            for machine, ip, error in failed:
                print(f"  {machine} ({ip}): {error}")
    else:
        print('Files Identical, No Need to Replace')

    # Check Maintenance Directory for New Files
    check_maint_files(maintpath, quepath)
    # update_scrdb(get_file_screws(partdatapath))
    msg = "Total Time = " + str(round((timer() - start), 3)) + " sec"
    logger.info(msg)
    print(msg)
    return


if __name__ == '__main__':
    main()