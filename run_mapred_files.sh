#bash script to run map-reduce via Streaming API

#export INPUT_PATH='/user/oracle/police_data/2014-09*'
#export OUTPUT_PATH='/user/oracle/crime_sum1'

export STREAMJAR=/usr/lib/hadoop-mapreduce/hadoop-streaming.jar 

export MAPPER="/media/sf_Python/mapred/mapper.py"
export REDUCER="/media/sf_Python/mapred/reducer.py"


if [ $# -ne 2 ]
  then
  echo "Usage $0  <Hadoop Input Dir> <Hadoop Output Dir>"
  exit 1
fi

INPUT_PATH=$1
OUTPUT_PATH=$2

printf "hadoop jar $STREAMJAR \n -files $MAPPER, $REDUCER \n   -mapper $MAPPER \n  -reducer $REDUCER \n -input $INPUT_PATH -output $OUTPUT_PATH"

hadoop jar $STREAMJAR \
-files $MAPPER, $REDUCER    \
-mapper $MAPPER \
-reducer $REDUCER \
-input $INPUT_PATH \
-output $OUTPUT_PATH
