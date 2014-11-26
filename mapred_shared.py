'''
Created on 26 Nov 2014

Common functions for Python Hadoop Streaming API programs

@author: ebullen
'''
import sys

def printout(text):
    """
    Very basic print-line for version 2 / 3 compatibility
    """
    text += "\n"
    sys.stdout.write(text)

#list of crime categories that the mapper can output
crimes_list=("bicycle_theft",
                   "social",
                   "burglary",
                   "damage_or_arson", 
                   "drugs",
                   "other_theft",
                   "weapons",
                   "public_order",
                   "shoplifting", 
                   "robbery", 
                   "theft_person",
                   "vehicle_crime",
                   "violence_sex", 
                   "other_crime",
                   "unclassified",
                   "missing_data")

    
def init_crimes():
    """
    Returns a crimes dictionary, generated from the list of "crimes_list" with values all set to 0
    """
    # Python 2.7 syntax: crimes = {crime:0 for crime in crimes_list}
    
    # Python 2.6 Sytnax: 
    crimes= {}
    for crime in crimes_list:
        crimes[crime] = 0
    
    return(crimes)
    
