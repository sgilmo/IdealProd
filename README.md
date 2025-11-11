## Ideal Clamp Products Production Support System ##

**update.py**

> Parses CSV files and enters data into appropriate SQL Server tables. 
  Also moves camera files to engineering drive. 

**getclamps.py**

> Compares daily part data file "parts.csv" with FileMaker data, if there is a difference, 
  create new data file and send it out to all machines.

**get_parts.py**

> Compares daily part data file "parts.csv" with FileMaker data, if there is a difference, 
  create new data file and send it out to all machines. This version will replace 'getclamps.py eventually.

**spares.py**

> Tracks Engineering Generated spares thrugh the system from initial request until the items are in stock.

**acm_set.py**

> Writes to a DM Register on a list of ACM PLCs. Currently used to enable/disable chopper function

**network_connectivity_check.py**

> Checks network access to machines and thier networked subsystems


