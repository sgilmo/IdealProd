## Ideal Clamp Products Production Support System ##

**update.py**

> Parses CSV files and enters data into appropriate SQL Server tables. 
  Also moves camera files to engineering drive.
> Runs @ 7:30AM every day
  
**update_data.py**

> Retrieves component inventory data from AS400, as well, as clamp and band data from filemaker. 
> Creates SQL Server tables for all and generates a csv file called parts_clamps.csv and places 
> it in the acmparts folder on TN-DATACOLLECT2. That file is used to determine 
> if the current active parts.csv file is current 
> Runs 45 minutes after every hour

**get_parts.py**

> Compares daily part data file "parts.csv" with FileMaker data, if there is a difference, 
  create new data file and send it out to all machines.
> Runs @ 2:10AM every day

**spares.py**

> Tracks Engineering Generated spares through the system from initial request until the items are in stock.
> Runs @ 1:30AM every day

**acm_set.py**

> Writes to a DM Register on a list of ACM PLCs. Currently used to enable/disable chopper function

**network_connectivity_check.py**

> Checks network access to machines and their networked subsystems
> Runs @ 11:00PM every day
> 
**price_update.py**

> Gets latest pricing from Automation Direct and loads it into SQL Server table
> Runs @ 1:30AM every day

**file_cleanup_utility.py**

> Delete camera files older than N days from approved network shares.
> Runs @ 1:00AM every day
> 
**emps.py**

> Get employee information from AS400, update SQL Server database with that data,
and create a CSV file in the FTP root directory.
> Runs @ 9:00AM every Saturday

