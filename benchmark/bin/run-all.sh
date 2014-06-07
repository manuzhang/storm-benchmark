#!/bin/sh

DIR=`dirname "$0"`
DIR=`cd "${DIR}/.."; pwd`

. $DIR/conf/config.sh


for benchmark in `cat $DIR/conf/benchmarks.lst`; do
  if [[ $benchmark == \#* ]]; then
    continue
  else
    sh $DIR/$benchmark/run.sh
    result=$?
    if [ $result -ne 0 ]; then
      echo "ERROR: Storm job failed to run successfully." 
      exit $result
    fi
  fi
done

