#!/usr/bin/python

import sys
import re
import os
import logging
import csv

import mapred_shared



""" Hadoop Mapper
Pipe in crime_data from CSVs loaded into HDFS - ie
$ head /media/sf_DataSets/data.police.uk/2012-01/2012-01-avon-and-somerset-street.csv
Crime ID,Month,Reported by,Falls within,Longitude,Latitude,Location,LSOA code,LSOA name,Crime type,Last outcome category,Context
,2012-01,Avon and Somerset Constabulary,Avon and Somerset Constabulary,-2.516919,51.423683,On or near A4175,E01014399,Bath and North East Somerset 001A,Anti-social behaviour,,
,2012-01,Avon and Somerset Constabulary,Avon and Somerset Constabulary,-2.510162,51.410998,On or near Monmouth Road,E01014399,Bath and North East Somerset 001A,Anti-social behaviour,,


target is key-value summary where
KEY = month:lsoa_code
VALUE = crime

also tag on "lsoa_name" as 3rd element <KEY>    <VALUE>    <lsoa_name>

lsoa_code,      -- input FIELD 8
month,          -- input FIELD 2  

Use RegEx to match text in FIELD 10 and derive crime value:
anti_social,    -- derived from field 10  matches social 
bicycle_theft,	-- derived from field 10  matches "Bicycle"
burglary,   	-- derived from field 10  matches "Burglary"
damage_or_arson,-- derived from field 10  matches "Criminal"
drugs,   	    -- derived from field 10  matches "Drugs"
other_theft,  	-- derived from field 10  matches "Other" AND "Theft"
weapons,   	    -- derived from field 10  matches "Weapons"
public_order,   -- derived from field 10  matches "Order" AND "Public"
robbery,   	    -- derived from field 10  matches "Robbery"
shoplifting,   	-- derived from field 10  matches "Shoplifting
theft_person,   -- derived from field 10  matches "Theft" AND "Person"
vehicle_crime,  -- derived from field 10  matches "Vehicle"
violence_sex,   -- derived from field 10  matches "Sex"
other_crime,   	-- derived from field 10  matches "Other" AND "Crime"

"""


### Main ###

pid = str(os.getpid())
i = 0
missing = 0
unclassified = 0
lsoa_code = ""; last_lsoa = ""
lsoa_name = ""

# Logging #
logging.basicConfig(filename='/tmp/mapper.log',
                    level=logging.INFO, 
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

logging.info("----------Start  PID=%s-------------------", pid)
logging.info("|")
###########

try:
    year_flag = 0
    if len(sys.argv) == 2:
        #Check for YEAR aggregation setting
        if sys.argv[1] == "YEAR":
            year_flag = 1
    
    
    for line in sys.stdin:
        i += 1
        line = line.strip()
        
        for row in csv.reader([line], skipinitialspace=True):
            unpacked = row
       
        #unpacked = line.split(",")
        unpacked = [item.lower() for item in unpacked] # set all strings lowercase
        if i == 1:
            logging.info("| First line is %s", line)
        
        date = str(unpacked[1])
        if year_flag == 1:                        #if the YEAR flag set, strip off month
            date = date.split("-")[0] 
    
        lsoa_code = str(unpacked[7])
        #if lsoa_code != last_lsoa:  # simple way to make sure lsoa_name is the same for all lsoa  
        lsoa_name = unpacked[8] # populate each time we start processing new LSOA
                    
        crime_type = unpacked[9]
        crime = ""
    
    

    
        #if unpacked[9] includes "bicycle", bicycle_theft = 1
        if re.search("bicycle", crime_type,):
            crime = mapred_shared.crimes_list[0]
        elif re.search("social", crime_type,):
            crime = mapred_shared.crimes_list[1]
        elif re.search("burglary", crime_type,): 
            crime = mapred_shared.crimes_list[2]
        elif re.search("criminal", crime_type,):
            crime = mapred_shared.crimes_list[3]   
        elif re.search("drugs", crime_type,):
            crime = mapred_shared.crimes_list[4]            
        elif (re.search("other", crime_type,)) and  (re.search("theft", crime_type,)):
            crime = mapred_shared.crimes_list[5]
        elif re.search("weapons", crime_type,):
            crime = mapred_shared.crimes_list[6]
        elif (re.search("order", crime_type,)) and (re.search("public", crime_type,)):
            crime = mapred_shared.crimes_list[7]
        elif re.search("shoplifting", crime_type,):
            crime = mapred_shared.crimes_list[8]
        elif re.search("robbery", crime_type,):
            crime = mapred_shared.crimes_list[9]
        elif (re.search("theft", crime_type,)) and (re.search("person", crime_type,)) :
            crime = mapred_shared.crimes_list[10]
        elif re.search("vehicle", crime_type,):
            crime = mapred_shared.crimes_list[11]
        elif (re.search("sex", crime_type,)) or (re.search("violen", crime_type,)) :
            crime = mapred_shared.crimes_list[12]
        elif (re.search("other", crime_type,)) and (re.search("crime", crime_type,)) :
            crime = mapred_shared.crimes_list[13]
        else:
            crime = mapred_shared.crimes_list[14]   # crime is "unclassified"
            unclassified += 1
            
        if lsoa_code == "":
            lsoa_code = "MISSING_LSOA_DATA"                  #MISSING
            crime = mapred_shared.crimes_list[14]  # Crime is "missing data"
        if date == "":
            date = "MISSING_DATE_DATA"                       #MISSING
            crime = mapred_shared.crimes_list[14]  # Crime is "missing data"
        if crime == mapred_shared.crimes_list[14]:
            missing += 1
        
            
        key = ":".join((date, lsoa_code))
        value = crime
        last_lsoa = lsoa_code
    
        #printv(key + "\t" + value)
        if not lsoa_name:
            lsoa_name = "NULL"
        mapred_shared.printout(key + "\t" + value + "\t" + lsoa_name)
            
except Exception as err:
    listlen = str(len(unpacked))
    logging.error("Error processing line with %s fields: %s", str(listlen), str(line))
    
    logging.error("---------Error Stack--------------")
    logging.error('Failed with error:', exc_info=True)
    logging.error("----------------------------------")
    
    raise

logging.info("|")
logging.info("Processed %s lines; %s missing data, %s unclassified crimes", i, missing, unclassified)
logging.info("|")
logging.info("| Final line is %s", line)
logging.info("----------End    PID=%s-------------------", pid)   

