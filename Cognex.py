import telnetlib
import csv
import subprocess

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


def set_camera_job(host, job_name):
# Set camera job
    port = 23 # Appropriate port
    command = job_name + "\r\n"        # Replace with the command you want to send
    user = "admin"
    tn = telnet_connect(host, port)
    temp = tn.read_until(b"\n").decode('utf-8')
    telnet_user = user+'\r\n'
    tn.write(telnet_user.encode('ascii')) #the user name is admin
    tn.write("\r\n".encode('ascii')) #there is no password - just return - now logged in
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
    return


def read_csv_to_list(file_path):
    data_list = []

    with open(file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        for row in csv_reader:
            data_list.append(row)

    return data_list



