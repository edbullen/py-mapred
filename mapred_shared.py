'''
Created on 26 Nov 2014

Common functions for Python Hadoop Streaming API programs

@author: ebullen
'''

import datetime

DATETIME = str(datetime.datetime.now())
DATETIME = DATETIME[0:10]+"_"+DATETIME[11:19]

MAPLOG = "/tmp/mapper_log." + DATETIME + ".out"
REDLOG = "/tmp/reducer_log." + DATETIME + ".out"

#list of crime categories that the mapper can output
crimes_list=("bicycle_theft",       #0
                   "social",        #1
                   "burglary",      #2
                   "damage_or_arson", #3
                   "drugs",         #4
                   "other_theft",   #5
                   "weapons",       #6
                   "public_order",  #7 
                   "shoplifting",   #8 
                   "robbery",       #9
                   "theft_person",  #10
                   "vehicle_crime", #11 
                   "violence_sex",  #12
                   "other_crime",   #13
                   "unclassified",  #14
                   "missing_data")  #15

    
def init_crimes():
    """
    Returns a crimes dictionary, generated from the list of "crimes_list" with values all set to 0
    """
    # Python 2.7 syntax: crimes = {crime:0 for crime in crimes_list}
    
    # Python 2.6 Syntax: 
    crimes= {}
    for crime in crimes_list:
        crimes[crime] = 0
    
    return(crimes)
    
