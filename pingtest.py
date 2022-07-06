#!/usr/bin/env python
# coding=utf-8

"""This program scans active equipment for network connectivity."""

from subprocess import Popen, PIPE
import CommonFunc

# create dictionary structure for machine IP addresses

machines = {'SMYRNA GATEWAY': '10.143.12.2', 'CG002_HMI': '10.143.50.123',
            'ACM365_HMI': '10.143.50.57', 'ACM365_PLC': '10.143.50.56',
            'ACM366_HMI': '10.143.50.59', 'ACM370_HMI': '10.143.50.67',
            'ACM362_HMI': '10.143.50.53', 'ACM369_HMI': '10.143.50.65',
            'P110_HMI': '10.143.50.167', 'ACM353_HMI': '10.143.50.161',
            'ACM361_HMI': '10.143.50.51', 'ACM355_HMI': '10.143.50.165',
            'ACM354_HMI': '10.143.50.163', 'ACM351_HMI': '10.143.50.157',
            'ACM350_HMI': '10.143.50.155', 'LACM384_HMI': '10.143.50.21',
            'LACM383_HMI': '10.143.50.23', 'ACM372_HMI': '10.143.50.69',
            'ACM373_HMI': '10.143.50.71', 'ACM367_HMI': '10.143.50.61',
            'ACM374_HMI': '10.143.50.73', 'ACM375_HMI': '10.143.50.75',
            'LACM390_HMI': '10.143.50.27', 'LACM391_HMI': '10.143.50.31',
            'LACM385_HMI': '10.143.50.13', 'LACM381_HMI': '10.143.50.15',
            'LACM382_HMI': '10.143.50.17', 'LACM386_HMI': '10.143.50.19',
            'ACM376_HMI': '10.143.50.77', 'LACM388_HMI': '10.143.50.87',
            'SLACM392_HMI': '10.143.50.203', 'LACM387_HMI': '10.143.50.25',
            'CG002_PLC': '10.143.50.122', 'P110_PLC': '10.143.50.166',
            'ACM366_PLC': '10.143.50.58', 'ACM370_PLC': '10.143.50.66',
            'ACM362_PLC': '10.143.50.52', 'ACM369_PLC': '10.143.50.64',
            'ACM353_PLC': '10.143.50.160',
            'ACM361_PLC': '10.143.50.50', 'ACM355_PLC': '10.143.50.164',
            'ACM354_PLC': '10.143.50.162', 'ACM351_PLC': '10.143.50.156',
            'ACM350_PLC': '10.143.50.154', 'LACM384_PLC': '10.143.50.20',
            'LACM383_PLC': '10.143.50.22', 'ACM372_PLC': '10.143.50.68',
            'ACM373_PLC': '10.143.50.70', 'ACM367_PLC': '10.143.50.60',
            'ACM374_PLC': '10.143.50.72', 'ACM375_PLC': '10.143.50.74',
            'LACM390_PLC': '10.143.50.26', 'LACM391_PLC': '10.143.50.30',
            'LACM385_PLC': '10.143.50.12', 'LACM381_PLC': '10.143.50.14',
            'LACM382_PLC': '10.143.50.16', 'LACM386_PLC': '10.143.50.18',
            'ACM376_PLC': '10.143.50.76', 'LACM388_PLC': '10.143.50.86',
            'SLACM392_PLC': '10.143.50.202', 'LACM387_PLC': '10.143.50.24',
            'CG001_PLC': '10.143.50.120', 'CG001_HMI': '10.143.50.121',
            'PVT001_PLC': '10.143.15.168', 'PVT001_HMI': '10.143.15.169',
            'ACM363_PLC': '10.143.50.54', 'ACM363_HMI': '10.143.50.55',
            }


# Define some functions


# Scan list of IP addresses for bad connections.
badlist = []
for hostName, hostIP in list(machines.items()):
    cmd = Popen("ping -n 1 " + hostIP, stdout=PIPE)
    for line in cmd.stdout:
        if "time=" in str(line):
            hostState = "Alive"
            break
        elif "unreachable" in str(line):
            hostState = "Down"
            badlist.append(hostName + " At " + hostIP + " [" + hostState + "]")
            break
        elif "timed out" in str(line):
            hostState = "Down"
            badlist.append(hostName + " At " + hostIP + " [" + hostState + "]")
            break

# If there were bad connections let us know about it
if len(badlist) > 0:
    print(str(len(badlist)) + ' Bad Connections' + " " + str(badlist))
    # Send the Email
    CommonFunc.send_email(badlist, "Network Connection Failure",
                          "The Following Devices are off the network:\n\n")
