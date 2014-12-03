
Example of Hadoop Streaming API and Python Map-Reduce
=====================================================

##Overview
Work with the UK police crime data taken from here:
http://data.police.uk/data/

CSV files are copied into a Hadoop FS directory (/user/oracle/police_data in this example)

Data in the Police Data files in this format:

+ Crime ID,Month,Reported by,Falls within,Longitude,Latitude,Location,LSOA code,LSOA name,Crime type,Last outcome category,Context
+ ,2012-01,Avon and Somerset Constabulary,Avon and Somerset Constabulary,-2.516919,51.423683,On or near A4175,E01014399,Bath and North East Somerset 001A,Anti-social behaviour,,

+,2012-01,Avon and Somerset Constabulary,Avon and Somerset Constabulary,-2.510162,51.410998,On or near Monmouth Road,E01014399,Bath and North East Somerset 001A,Anti-social behaviour,,
+ ...
+ ...
gets mapped to 

| <YEAR-MON>:LSOA   | <tab> | <Crime-Class>
| ----------------- | ----- | --------------
| 2012-01:e01029293 |       | damage_or_arson
| 2012-01:e01029293 |       | drugs
| 2012-01:e01029293 |       | drugs
| 2012-01:e01029293 |       | other_theft
| 2012-01:e01029293 |       | unclassified
| 2012-01:e01029293 |       | unclassified
| 2012-01:e01029293 |       | unclassified
| 2012-01:e01029293 |       | other_crime
| 2012-01:e01029266 |       | social
| 2012-01:e01029266 |       | damage_or_arson
| 2012-01:e01029284 |       | social

then reduced to

Count up the crimes by class and summarise by the key:

|DATE,   | LSOA     ,| crime[0],| crime[1],| crime[2], |crime[3],|...|crime[n]|
|--------|-----------|----------|----------|-----------|---------|---|--------|
|2012-01,| e01029293,| 1       ,| 2       ,| 0       , |0   ,    |...| 4      |

(see the definition of crime-types 0..n in the mapred_shared.py file; list def "crimes_list")

##Setup
Copy the files
+ reducer.py
+ mapper.py
+ mapred_shared.py
+ run_mapred.sh
to an appropriate directory.

I copied to /tmp as a quick hack; sub-dirs need to be created by Yarn from what I can see, so proper way to do this is put files in correct place in your Hadoop installation.

##Execution

Execute the supplied "mapper.py" and "reducer.py" Python utilities via the Hadoop Streaming API (tested against Hadoop Version 2.3.0-cdh5.1.2)

A supplied bash shell script can be used to call the mapper.py and reducer.py with correct Hadoop arguments with specificed source and dest Hadoop directories.

NOTE: when using the "file" option for the input file spec, do not specific a directory; needs a wild-card spec for a set of files _in_ a directory.


