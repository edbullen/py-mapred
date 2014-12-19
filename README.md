
Example of Hadoop Streaming API and Python Map-Reduce
=====================================================

##Overview
Work with the UK police crime data taken from here:
http://data.police.uk/data/

CSV files are copied into a Hadoop FS directory (/user/oracle/police_data in this example)

Data in the Police Data files in this format:

+ Crime ID,Month,Reported by,Falls within,Longitude,Latitude,Location,LSOA code,LSOA name,Crime type,Last outcome category,Context
+ ,2012-01,Avon and Somerset Constabulary,Avon and Somerset Constabulary,-2.516919,51.423683,On or near A4175,E01014399,Bath and North East Somerset 001A,Anti-social behaviour,,
+ ,2012-01,Avon and Somerset Constabulary,Avon and Somerset Constabulary,-2.510162,51.410998,On or near Monmouth Road,E01014399,Bath and North East Somerset 001A,Anti-social behaviour,,
+ ...etc...
+ ...etc...

The "LSOA" refered to is "Lower Layer Super Output Area (LSOA) boundaries".  More info here:
http://www.ons.gov.uk/ons/guide-method/geography/beginner-s-guide/census/super-output-areas--soas-/index.html

There is a source of UK postcode to LSOA mapping here:
http://opendatacommunities.org/data/postcodes



This gets **mapped** to 

| YEAR-MON:LSOA     | tab   | Crime-Class    | LSOA_Name
| ----------------- | ----- | ---------------|----------
| 2012-01:e01029293 |       | damage_or_arson| <LSOA name>
| 2012-01:e01029293 |       | drugs          | <LSOA name>
| 2012-01:e01029293 |       | drugs          | <LSOA name>
| 2012-01:e01029293 |       | other_theft    | <LSOA name>
| 2012-01:e01029293 |       | unclassified   | <LSOA name>
| 2012-01:e01029293 |       | unclassified   | <LSOA name>
| 2012-01:e01029293 |       | unclassified   | <LSOA name>
| 2012-01:e01029293 |       | other_crime    | <LSOA name>
| 2012-01:e01029266 |       | social         | <LSOA name>
| 2012-01:e01029266 |       | damage_or_arson| <LSOA name>
| 2012-01:e01029284 |       | social         | <LSOA name>

then reduced to

Count up the crimes by class and summarise by the key:

|DATE,   | LSOA     ,| LSOA_Name   | crime[0],| crime[1],| crime[2], |crime[3],|...|crime[n]|
|--------|-----------|-------------|----------|----------|-----------|---------|---|--------|
|2012-01,| e01029293,| <LSOA_Name>,|1        ,| 2       ,| 0       , |0   ,    |...| 4      |

(see the definition of crime-types 0..n in the mapred_shared.py file; list def "crimes_list")

##Setup
Copy the files
+ reducer.py
+ mapper.py
+ mapred_shared.py
+ run_mapred.sh

to an appropriate directory.

I copied to /tmp as a quick hack; sub-dirs need to be created by Yarn, so ideally put files in correct place in your Hadoop installation.

##Execution

Execute the supplied "mapper.py" and "reducer.py" Python utilities via the Hadoop Streaming API (tested against Hadoop Version 2.3.0-cdh5.1.2)

A supplied bash shell script can be used to call the mapper.py and reducer.py with correct Hadoop arguments with specificed source and dest Hadoop directories.

NOTE: when using the "file" option for the input file spec, do not specify a directory; needs a wild-card spec for a set of files _in_ a directory.

###Example

[oracle@bigdatalite mapred]$ ./run_mapred.sh /tmp/mapper.py /tmp/reducer.py /user/oracle/police_data/*.csv /user/oracle/crime_sum

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar
 -file /tmp/mapper.py    -mapper python /tmp/mapper.py
 -file /tmp/reducer.py   -reducer python /tmp/reducer.py
 -input /user/oracle/police_data/*.csv -output /user/oracle/crime_sum

14/12/08 05:33:35 WARN streaming.StreamJob: -file option is deprecated, please use generic option -files instead.
packageJobJar: [/tmp/mapper.py, /tmp/reducer.py] [/usr/lib/hadoop-mapreduce/hadoop-streaming-2.3.0-cdh5.1.2.jar] /tmp/streamjob4885598677458329955.jar tmpDir=null
14/12/08 05:33:36 INFO client.RMProxy: Connecting to ResourceManager at localhost/127.0.0.1:8032
14/12/08 05:33:37 INFO client.RMProxy: Connecting to ResourceManager at localhost/127.0.0.1:8032
...
... etc
14/12/08 07:16:26 INFO mapreduce.Job:  map 100% reduce 100%
14/12/08 07:16:28 INFO mapreduce.Job: Job job_1417792229125_0001 completed successfully
14/12/08 07:16:28 INFO mapreduce.Job: Counters: 49
...
...
        File Output Format Counters
                Bytes Written=79461938
14/12/08 07:16:28 INFO streaming.StreamJob: Output directory: /user/oracle/crime_sum

###Example - create summary aggregated by YEAR not default month
[oracle@bigdatalite mapred]$ ./run_mapred.sh "/tmp/mapper.py YEAR" /tmp/reducer.py /user/oracle/police_data/*.csv /user/oracle/crime_sum_year



##Setup Hive Table

There are two options on the reducer.py program:
+ python reducer.py print_header
+ python reducer.py print_cols
run the reducer.py util at the command line to print out a definition of the data to map to a Hive table.

Alternatively, script
+ cr_crime_sum_by_lsoa.sh
can be executed to create a Hive table that maps to the output of the map-reduce job in a specified Hadoop dir.

Example:
./cr_crime_sum_by_lsoa.sh CRIME_SUM /user/oracle/crime_sum

creates an Hive table "CRIME_SUM" mapped to the files in HDFS /user/oracle/crime_sum directory.
hive> describe CRIME_SUM;

date                    string                  None
lsoa                    string                  None
lsoa_name               string                  None
bicycle_theft           int                     None
burglary                int                     None
damage_or_arson         int                     None
drugs                   int                     None
missing_data            int                     None
other_crime             int                     None
other_theft             int                     None
public_order            int                     None
robbery                 int                     None
shoplifting             int                     None
social                  int                     None
theft_person            int                     None
unclassified            int                     None
vehicle_crime           int                     None
violence_sex            int                     None
weapons                 int                     None
total_classified        int                     None
Time taken: 1.093 seconds, Fetched: 20 row(s)
hive>








