#!/usr/bin/python

import sys
import logging
import os

import mapred_shared



""" Hadoop Reducer
Pipe in crime_data from Mapper in this fmt:
2012-01:e01029293       damage_or_arson    bristol 001c
2012-01:e01029293       drugs    bristol 001c
2012-01:e01029293       drugs    bristol 001c
2012-01:e01029293       other_theft    bristol 001c
2012-01:e01029293       unclassified    bristol 001c
2012-01:e01029293       unclassified    bristol 001c
2012-01:e01029293       unclassified    bristol 001c
2012-01:e01029293       other_crime    bristol 001c
2012-01:e01029266       social    bristol 032f
2012-01:e01029266       damage_or_arson    bristol 032f
2012-01:e01029284       social     bristol 019a

MAPPER is written so no ":" in free text ... we use this as a key sub-divide in
REDUCER logic

KEY = month:lsoa_code
VALUE = crime class

Count up the crimes by class and summarise by the key:
DATE,    LSOA     , crime[0], crime[1], crime[2], crime[3]....crime[n]
2012-01, e01029293, 1       , 2       , 0       , 0   ...     4

"""

def sum_crimes(crimes):  # sum all classified crimes
    s = 0
    for a in (crimes):
        if (a != "unclassified") and (a != "missing_data"):
            s += crimes[a]
    return s

def print_output(key, lsoa_name, crimes):
    text = ""
    s = sum_crimes(crimes)
    for crime in sorted(crimes):
        #text += crime + ":"   # debug
        text += str(crimes[crime])
        text += ","   # add a comma separator
    text += str(s)
    text = text.strip(' \t\n\r')
    mapred_shared.printout(",".join(key) + "," + lsoa_name + "," + text + ", ENDOFLINE")
    
def print_header(crimes):
    text = "date,lsoa,lsoa_name,"
    for crime in sorted(crimes):
        text += crime
        text += ","   # add a comma separator
    text += "total_classified"
    mapred_shared.printout(text)    

# pretty much a duplicate of  print_header ... tidy-up todo!
def print_col_defs(crimes):
    text = "(date     STRING, \nlsoa    STRING,\nlsoa_name    STRING,\n"    
    for crime in sorted(crimes):
        text += crime
        text += "    INT,\n"   
    text += "total_classified    INT)"
    mapred_shared.printout(text)


# Main #

pid = str(os.getpid())
i = 0  # count of lines processed
j = 0  # count of rows output

crime = ""                             # crime type being processed
lsoa_name = ""
last_lsoa_name = ""
crimes = mapred_shared.init_crimes()   # dictionary of crime-counts for each type
key = []
lastkey = []


# Logging #
logging.basicConfig(filename='/tmp/reducer.log',
                    level=logging.INFO, 
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

logging.info("----------Start  PID=%s-------------------", pid)
logging.info("|")

if len(sys.argv) == 2:
    if sys.argv[1] == "print_header":
        print_header(crimes)
    elif sys.argv[1] == "print_cols":
        print_col_defs(crimes)
else:
    try:
   
        for line in sys.stdin:
            i += 1
            line = line.strip()
            
            key, crime, lsoa_name = line.split("\t")
            key = key.split(":")  # list with [0,1] elements
            
            if i == 1:
                logging.info("| First key-item is %s", ":".join(key))
                
            if not lastkey:         # if lastkey is null, initialise it
                lastkey = key       # should only happen at start
        
            # this IF-switch only works because Hadoop sorts map output by key before passing to reducer
            if "".join(key) == "".join(lastkey):  # while the key same as last key, count up crimes by type
                crimes[crime] = crimes.get(crime,0) + 1 # add 1 to crimes dict ref'd for this crime type
                lastkey = key
                last_lsoa_name = lsoa_name 
            else:                  # We've hit a condition where key changes, print results
               
                #mapred_shared.printout(str(key) + str(lastkey)) #DEBUG
                if ":".join(lastkey) != "month:lsoa code":      # quick fix - ignore CSV headers                                                 
                    print_output(lastkey, last_lsoa_name, crimes)
                    j += 1
                    if j == 1:        
                        logging.info("| Second key-set (record %s) is %s", i, ":".join(key))
            
        
                #reset crime counters before we move on to process with a new sub-key
                crimes = mapred_shared.init_crimes()
                crimes[crime] = crimes.get(crime,0) + 1 # add "value" to crimes dict ref'd for this crime type
                
                lastkey = key
                
        
        # when we hit the end of the file, print the summary for final sub-key
        if ":".join(lastkey) != "month:lsoa code":      # quick fix - ignore CSV headers
            print_output(lastkey, lsoa_name, crimes)
            j += 1
        
    except Exception as err:
        
        logging.error("Error processing key %s ", ":".join(key))
        logging.error("line %s",line)
        
        logging.error("---------Error Stack--------------")
        logging.error('Failed with error:', exc_info=True)
        logging.error("----------------------------------")
        
        raise

logging.info("Processed %s lines, summarised to %s rows", i, j)
logging.info("|")
logging.info("| Final key is %s", ":".join(key))
logging.info("|")
logging.info("----------End    PID=%s-------------------", pid)   
