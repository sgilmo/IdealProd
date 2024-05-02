import time
import telnetlib
import csv
import os
import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


def read_csv_to_list(file_path):
    data_list = []
    assert os.path.isfile(file_path)
    with open(file_path, 'rt', newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            data_list.append(row)
    return data_list


# Function to run when a new file is added
def process_new_file(filename):
    print(f"New file added: {filename}")
    time.sleep(3)  # Hold off for 3 seconds to give the file time to fully transfer
    job_data = read_csv_to_list(filename)
    set_camera_job(job_data, filename)


# Watcher event handler
class MyHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        process_new_file(event.src_path)


def set_camera_job(job_data, filename):
    # Set camera job
    port = 23
    for item in job_data:
        host = item[2]  # Host Address
        job_name = item[3] + '_cam.job'
        # print('File Data: ' + item)
        command = "lf" + job_name + '\r\n'
        user = "admin"
        tn = telnet_connect(host, port)
        temp = tn.read_until(b"\n").decode('utf-8')
        print(temp)
        telnet_user = user+'\r\n'
        tn.write(telnet_user.encode('ascii'))
        tn.write('\r\n'.encode('ascii'))  # there is no password - just return - now logged in
        print('Telnet Logged in')
        print("Setting Timeout")
        tn.write(b"PUT Timeout 60000\r\n")
        if tn:
            send_command(tn, command)
            response = read_response(tn)
            print("Response1:", response)
            response = read_response(tn)
            print("Response2:", response)
            tn.close()
            build_file_name(item, filename)
    return


def build_file_name(job_data, filename):
    # Set Archive Path
    archive_path = "\\\\Tn-datacollect2\\c\\inetpub\\ftproot\\monitor_archive\\"
    # Get the current date and time
    current_datetime = datetime.datetime.now()

    # Extract year, month, day, hour, and minute components
    year = current_datetime.year
    month = current_datetime.month
    day = current_datetime.day
    hour = current_datetime.hour
    minute = current_datetime.minute

    # Create the text-based date code
    date_code = f"{year}{month:02d}{day:02d}{hour:02d}{minute:02d}"
    # Rename and move data file to archive folder
    os.rename(filename, archive_path + job_data[0] + "_" + date_code + ".csv")
    return


def telnet_connect(host, port):
    try:
        # Connect to the Telnet server
        tn = telnetlib.Telnet(host, port)
        return tn
    except Exception as e:
        print("Error:", e)
        return None


def send_command(tn, command):
    try:
        # Send the command to the Telnet server
        tn.write(command.encode('utf-8') + b"\n")
    except Exception as e:
        print("Error:", e)


def read_response(tn):
    try:
        # Read the response from the Telnet server
        response = tn.read_until(b"\n").decode('utf-8')
        return response
    except Exception as e:
        print("Error:", e)
        return None


if __name__ == "__main__":
    # Directory to monitor
    directory_to_watch = "\\\\Tn-datacollect2\\c\\inetpub\\ftproot\\monitor"

    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, directory_to_watch, recursive=False)

    print(f"Monitoring directory: {directory_to_watch}")

    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
