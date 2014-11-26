#!/usr/bin/python

import sys
import logging
import os

import mapred_shared



""" Hadoop Reducer
Pipe in crime_data from Mapper in this fmt:
2012-01:e01029293       damage_or_arson
2012-01:e01029293       drugs
2012-01:e01029293       drugs
2012-01:e01029293       other_theft
2012-01:e01029293       unclassified
2012-01:e01029293       unclassified
2012-01:e01029293       unclassified
2012-01:e01029293       other_crime
2012-01:e01029266       social
2012-01:e01029266       damage_or_arson
2012-01:e01029284       social

MAPPER is written so no ":" in free text ... we use this as a key sub-divide in
REDUCER logic

KEY = month:lsoa_code
VALUE = crime class

Count up the crimes by class and summarise by the key:
DATE,    LSOA     , crime[0], crime[1], crime[2], crime[3]....crime[n]
2012-01, e01029293, 1       , 2       , 0       , 0   ...     4

"""

#def init_crimes(crimes):
#    crimes = {"bicycle_theft": 0, "social": 0, "burglary": 0, "damage_or_arson": 0, "drugs": 0, "other_theft": 0, "weapons": 0, "public_order": 0, "shoplifting": 0, "robbery":0, "theft_person": 0, "vehicle_crime": 0, "violence_sex": 0, "other_crime": 0}
#    return(crimes)

def print_output(key, crimes):
    text = ""
    for crime in sorted(crimes):
        text += str(crimes[crime])
        text += ","   # add a comma separator
    if len(text) > 0:
        if text[-1] == "," : text = text[:-1]   # remove the trailing comma
    #DEBUG print sorted(crimes)# print len(crimes)
    mapred_shared.printout(",".join(key) + "," + text)

def print_header(crimes):
    text = ""
    for a in sorted(crimes):
        text += a
        text += ","   # add a comma separator
    if len(text) > 0:
        if text[-1] == "," : text = text[:-1]   # remove the trailing comma
    mapred_shared.printout("month" + "," + "lsoa" +  "," + text)    


# Main #
pid = str(os.getpid())
i = 0
j = 0

# Logging #
logging.basicConfig(filename='/tmp/reducer.log',
                    level=logging.INFO, 
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

logging.info("----------Start  PID=%s-------------------", pid)
logging.info("|")
###########


crime = ""   # crime type being processed
crimes = mapred_shared.init_crimes()   # dictionary of crime-counts
key = []
lastkey = []

#subkey = []  # split the key into month,lsoa parts as a "sub-key" 
            
      
try:

    for line in sys.stdin:
        i += 1
        line = line.strip()
        
        key,crime = line.split("\t")
        key = key.split(":")  # list with [0,1] elements
        
        if i == 1:
            logging.info("| First key-item is %s", ":".join(key))
            
        if not lastkey:         # if lastkey is null, initialise it
            lastkey = key       # should only happen at start
    
        # this IF-switch only works because Hadoop sorts map output by key before passing to reducer
        if "".join(key) == "".join(lastkey):  # while the key same as last key, count up crimes by type
            crimes[crime] = crimes.get(crime,0) + 1 # add 1 to crimes dict ref'd for this crime type
            lastkey = key 
        else:                  # We've hit a condition where key changes, print results
           
            #mapred_shared.printout(str(key) + str(lastkey)) #DEBUG
            if ":".join(lastkey) != "month:lsoa code":      # quick fix - ignore CSV headers                                                 
                print_output(lastkey, crimes)
                j += 1
                if j == 1:        
                    logging.info("| Second key-set (record %s) is %s", i, ":".join(key))
        
    
            #reset crime counters before we move on to process with a new sub-key
            crimes = mapred_shared.init_crimes()
            crimes[crime] = crimes.get(crime,0) + 1 # add "value" to crimes dict ref'd for this crime type
            
            lastkey = key
            
    
    # when we hit the end of the file, print the summary for final sub-key
    if ":".join(lastkey) != "month:lsoa code":      # quick fix - ignore CSV headers
        print_output(lastkey, crimes)
        j += 1
    
    #print_header(crimes)
    
    

except Exception as err:
    
    logging.error("Error processing key %s ", ":".join(key))
    
    logging.error("---------Error Stack--------------")
    logging.error('Failed with error:', exc_info=True)
    logging.error("----------------------------------")
    
    raise

logging.info("Processed %s lines, summarised to %s rows", i, j)
logging.info("|")
logging.info("| Final key is %s", ":".join(key))
logging.info("|")
logging.info("----------End    PID=%s-------------------", pid)   