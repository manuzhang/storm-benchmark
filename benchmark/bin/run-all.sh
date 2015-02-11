#!/bin/sh

DIR=`dirname "$0"`
DIR=`cd "${DIR}/.."; pwd`

. $DIR/conf/config.sh

while read benchmark; do
  if [[ ${benchmark:0:1} == '#' ]]; then
    continue
  else
    sh $DIR/$benchmark/run.sh
    result=$?
    if [ $result -ne 0 ]; then
      echo "ERROR: Storm job failed to run successfully." 
      exit $result
    fi
    sleep 30s
  fi
done <$DIR/conf/benchmarks.lst

