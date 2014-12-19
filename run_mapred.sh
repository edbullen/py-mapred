#bash script to run map-reduce via Streaming API

#export INPUT_PATH='/user/oracle/police_data/2012*'
#export OUTPUT_PATH='/user/oracle/crime_sum1'
export STREAMJAR=/usr/lib/hadoop-mapreduce/hadoop-streaming.jar 

#export MAPPER=/media/sf_Python/mapred/mapper.py
#export REDUCER=/media/sf_Python/mapred/reducer.py


if [ $# -ne 4 ]
  then
  echo "Usage $0 <mapper.py> <reducer.py> <Hadoop Input Dir/file_spec*> <Hadoop Output Dir>"
  #echo "Usage $0 <Hadoop Input Dir> <Hadoop Output Dir>"
  exit 1
fi

MAPPER=$1
MAPPERFILE=`echo $1 | cut -d " " -f1` #if an arg was specificed, split it off
REDUCER=$2
INPUT_PATH=$3
OUTPUT_PATH=$4

printf "\nhadoop jar $STREAMJAR \n -file $MAPPERFILE    -mapper python $MAPPER \n -file $REDUCER   -reducer python $REDUCER \n -input $INPUT_PATH -output $OUTPUT_PATH \n\n"

hadoop jar $STREAMJAR \
-file $MAPPERFILE    -mapper "python $MAPPER" \
-file $REDUCER   -reducer "python $REDUCER" \
-input $INPUT_PATH -output $OUTPUT_PATH
