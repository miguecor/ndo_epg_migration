# _NDO EPG MIGRATION_
A script that can generate an excel file with current information of the NDO tenants, sites, schemas and templates and
use that information as input to migrate BD/EPG pairs from one tenant to another with minimum impact to data traffic.

___
### Usage
The script can be run with the `--get/-g` flag to dump all the NDO data into an Excel file.  If no filename is provided
with the `--filename/-f` flag, the filename will be 'data.xlsx'.

Example:  
`python ndo_epg_migration --get --filename <filename>`  

The script can also be run with the `--put/-p` flag to use the 'EPG Selection' workbook in the excel file as input file 
for the migration of BD/EPGs.  If no filename is provided with the `--filename/-f` flag, the script will look for the 
'data.xlsx' file.
Example:  
`python ndo_epg_migration --put --filename <filename>`  

Running the script with either `-g` or `-p` flags will generate a .log directory with detailed information of the script 
process and results.

The script can also be run with the `--debug/-d` flag which will generate more verbose output to the console.
___
### Help
A help message is available:  
`python ndo_epg_migration --help`  

___
### Requirements
To install the necessary modules for the script run: 
`$ pip install -r requirements.txt`

___
### Important
This script was generated using python 3.11.  The script should be able to run with python 3.7 or later, but if 
you have any problem running it, try using a python environment with python 3.11 or later.

___