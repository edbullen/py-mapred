'''
Created on 26 Nov 2014

Common functions for Python Hadoop Streaming API programs

@author: ebullen
'''
import sys

if sys.version_info[0] < 3:
    from ConfigParser import *  # Python 2
else:
    from configparser import *   # Python 3


def getconfig(cname):   
    """
    Read the config file and populate a set of params.
    """
    Config = ConfigParser()
    configSections = {}   #{main: {p1:val, p2:val,...] next: [p1:val, p2:val,...]}
    
    Config.read(cname)
    for section in Config.sections():
        params = {}
        #configSections["section"] = section
        options = Config.options(section)
        for option in options:
            try:
                params[option] = Config.get(section,option)
            except:
                print("Error parsing config file: section {0}, option {1}".format(section, option))
        configSections[section] = params
        # add the params Dict to the configSections Dict
   
    return configSections


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
    
