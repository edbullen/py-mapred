#!/usr/bin/python

import sys
import re


""" Hadoop Reducer
Pipe in crime_data from Mapper in this fmt:
2012-01:e01029328:burglary      1
2012-01:e01029328:damage_or_arson       1
2012-01:e01029328:damage_or_arson       1
2012-01:e01029328:vehicle_crime 1
2012-01:e01029338:burglary      1

MAPPER is written so no ":" in free text ... we use this as a key separator in
REDUCER logic

KEY = month:lsoa_code:crime
VALUE = count (of crimes in the LSOA, for that month, of that type)

...and summarise it

"""

def printv(text):
    """
    Very basic print-line for version 2 / 3 compatibility
    """
    text += "\n"
    sys.stdout.write(text)

def init_crimes(crimes):
    crimes = {"bicycle_theft": 0, "social": 0, "burglary": 0, "damage_or_arson": 0, "drugs": 0, "other_theft": 0, "weapons": 0, "public_order": 0, "shoplifting": 0, "robbery":0, "theft_person": 0, "vehicle_crime": 0, "violence_sex": 0, "other_crime": 0}
    return(crimes)

def print_output(subkey, crimes):
    text = ""
    for crime in sorted(crimes):
        text += str(crimes[crime])
        text += ","   # add a comma separator
    if len(text) > 0:
        if text[-1] == "," : text = text[:-1]   # remove the trailing comma
    #DEBUG print sorted(crimes)# print len(crimes)
    printv(",".join(subkey) + "," + text)

def print_header(subkey, crimes):
    text = ""
    for key in sorted(crimes):
        text += key
        text += ","   # add a comma separator
    if len(text) > 0:
        if text[-1] == "," : text = text[:-1]   # remove the trailing comma
    printv("month" + "," + "lsoa" +  "," + text)
 

# Main #


log = open("/tmp/reducer.log", "a")

crimes = {}  # dict to store crimetype : count
crime = ""   # crime type being processed
crimes = init_crimes(crimes)

subkey = []  # split out the month,lsoa part as a "sub-key" 
             # then we print out the 13 crime categories and pivot them as 
             # as summary when we hit the next primary key

log.write("\n----------------------------------")
             
try:

    #print_header(subkey, crimes)
    for line in sys.stdin:
        line = line.strip()
    
        key,value = line.split("\t")
        key = key.split(":")  # list with [0,1,2] elements
        value = int(value)   # value is always going to be "1"?
    
        if not subkey:         # if subkey is null, initialise it
            subkey = key[0:2]  # should only happen at start
    
        crime = key[2] 
        # this IF-switch only works because Hadoop sorts map output by key before it is passed to the reducer
        if "".join(subkey) == "".join(key[0:2]):  # while the sub-key is the sam  as latest key,
                                                  # count up the crimes by type
            crimes[crime] = crimes.get(crime,0) + value # add "value" to crimes dict ref'd for this crime type
            subkey = key[0:2] # set sub-key
        else:                                    # We've hit a condition where sub-key schanges
                                                 # print out summary results for this sub-key
            
            # if we hit subkey != key, print out totals for crime for last subkey (date:lsoa)
            print_output(subkey, crimes)
    
            #reset crime counters before we move on to process with a new sub-key
            crimes = init_crimes(crimes)
            crimes[crime] = crimes.get(crime,0) + value # add "value" to crimes dict ref'd for this crime type
            
            subkey = key[0:2]
    
    # when we hit the end of the file, print the summary for final sub-key
    print_output(subkey, crimes)
    
    log.write("\n----------------------------------")

except Exception as err:
    exc_info = sys.exc_info()
    log.write("\n----------------------------------")
    log.write("------Error Stack-----------")
    log.write(err)
    log.close()
    
    raise exc_info[0], exc_info[1], exc_info[2]

log.close()