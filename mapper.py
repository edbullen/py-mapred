#!/usr/bin/python

import sys
import re
import os
import traceback


""" Hadoop Mapper
Pipe in crime_data from CSVs loaded into HDFS - ie
$ head /media/sf_DataSets/data.police.uk/2012-01/2012-01-avon-and-somerset-street.csv
Crime ID,Month,Reported by,Falls within,Longitude,Latitude,Location,LSOA code,LSOA name,Crime type,Last outcome category,Context
,2012-01,Avon and Somerset Constabulary,Avon and Somerset Constabulary,-2.516919,51.423683,On or near A4175,E01014399,Bath and North East Somerset 001A,Anti-social behaviour,,
,2012-01,Avon and Somerset Constabulary,Avon and Somerset Constabulary,-2.510162,51.410998,On or near Monmouth Road,E01014399,Bath and North East Somerset 001A,Anti-social behaviour,,


Eventual target is key-value summary where
KEY = month:lsoa_code:crime
VALUE = count (of crimes in the LSOA, for that month, of that type)

lsoa_code,       -- input field 8
lsoa_name,       -- input field 9
month,           -- input field 2  
anti_social,       [1/0] -- derived from field 10  matches social 
bicycle_theft,	   [1/0] -- derived from field 10  matches "Bicycle"
burglary,   	   [1/0] -- derived from field 10  matches "Burglary"
damage_or_arson,   [1/0] -- derived from field 10  matches "Criminal"
drugs,   	[1/0] -- derived from field 10  matches "Drugs"
other_theft,  	[1/0] -- derived from field 10  matches "Other" AND "Theft"
weapons,   	[1/0] -- derived from field 10  matches "Weapons"
public_order,   [1/0] -- derived from field 10  matches "Order" AND "Public"
robbery,   	[1/0] -- derived from field 10  matches "Robbery"
shoplifting,   	[1/0] -- derived from field 10  matches "Shoplifting
theft_person,   [1/0] -- derived from field 10  matches "Theft" AND "Person"
vehicle_crime,  [1/0] -- derived from field 10  matches "Vehicle"
violence_sex,   [1/0] -- derived from field 10  matches "Sex"
other_crime,   	[1/0] -- derived from field 10  matches "Other" AND "Crime"

"""

def printv(text):
    """
    Very basic print-line for version 2 / 3 compatibility
    """
    text += "\n"
    sys.stdout.write(text)

### Main ###

pid = str(os.getpid())
log = open("/tmp/mapper." + pid + ".log", "a")
log.write("\n----------Start---------------------")

try:
    year_flag = 0
    if len(sys.argv) == 2:
        #Check for YEAR aggregation setting
        if sys.argv[1] == "YEAR":
            year_flag = 1
    
    
    for line in sys.stdin:
        line = line.strip()
        unpacked = line.split(",")
        unpacked = [item.lower() for item in unpacked] # set all strings lowercase
        
        month = str(unpacked[1])
        if year_flag == 1:                        #if the YEAR flag set, strip off month
            month = month.split("-")[0] 
    
        lsoa_code = str(unpacked[7])
        #lsoa_name = unpacked[8]
        crime_type = unpacked[9]
        crime = ""
    
        #if unpacked[9] includes "bicycle", bicycle_theft = 1
        if re.search("bicycle", crime_type,):
            crime = "bicycle_theft"
    
        if re.search("social", crime_type,):
            crime = "social"
            
        if re.search("burglary", crime_type,): 
            crime = "burglary"
            
        if re.search("criminal", crime_type,):
            crime = "damage_or_arson"
            
        if re.search("drugs", crime_type,):
            crime = "drugs"
            
        if (re.search("other", crime_type,)) and  (re.search("theft", crime_type,)):
            crime = "other_theft"
            
        if re.search("weapons", crime_type,):
            crime = "weapons"
            
        if (re.search("order", crime_type,)) and (re.search("public", crime_type,)):
            crime = "public_order"
            
        if re.search("robbery", crime_type,):
            crime = "robbery"
            
        if re.search("shoplifting", crime_type,):
            crime = "shoplifting"
            
        if (re.search("theft", crime_type,)) and (re.search("person", crime_type,)) :
            crime = "theft_person"
            
        if re.search("vehicle", crime_type,):
            crime = "vehicle_crime"
    
        if re.search("sex", crime_type,) :
            crime = "violence_sex"
      
        if (re.search("other", crime_type,)) and (re.search("crime", crime_type,)) :
            crime = "other_crime"
            
        if (lsoa_code != "") and (crime != "") :  # some data has no location - skip it
            key = ":".join((month, lsoa_code, crime))
            value = "1"
    
            printv(key + "\t" + value)
            
except Exception as err:
    exc_info = sys.exc_info()
    log.write("\n---------Error Stack--------------\n")
    listlen = str(len(unpacked))
    log.write("unpacked list is " + listlen + " long")
    log.write("\nProcessing: " + str(line) + "\n")
    errstring=err.message
    errline=str(traceback.tb_lineno(sys.exc_info()[2]))
    
    log.write(errstring + " at line " + errline)
    log.write("\n----------------------------------\n")
    log.close()

    raise exc_info[0], exc_info[1], exc_info[2]

log.write("\n------------End---------------------\n")    
log.close()
