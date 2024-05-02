import csv
import os
import datetime


def read_csv_to_list(file_path):
    data_list = []
    with open(file_path, 'r', newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            data_list.append(row)
    return data_list

# Get the current date and time
current_datetime = datetime.datetime.now()

# Extract year, month, day, hour, and minute components
year = current_datetime.year
month = current_datetime.month
day = current_datetime.day
hour = current_datetime.hour
minute = current_datetime.minute

# Set Path to monitored folder
file_path = "\\\\Tn-datacollect2\\c\\inetpub\\ftproot\\monitor\\parts.csv"
archive_path = "\\\\Tn-datacollect2\\c\\inetpub\\ftproot\\monitor_archive\\"
# Create the text-based date code
date_code = f"{year}{month:02d}{day:02d}{hour:02d}{minute:02d}"
# Get file data
csv_data_list = read_csv_to_list(file_path)

for item in csv_data_list:
    print(item[2]) #Host Address
    print(item[3]+'_cam.job')  #Camera Job File Name

os.rename(file_path, archive_path + item[0] + "_" + date_code + ".csv")







# Print the generated date code
#print(item[0] + "_" + date_code + ".csv")