#!/bin/sh

APP_ID=application_1400224450350_0061
JAR=/root/storm-benchmark-0.9.0-Intel-jar-with-dependencies.jar
MAIN_CLASS=storm.benchmark.BenchmarkRunner

run_benchmark() {
  check
  run
  kill_topology
}

check() {
  if [ -z $APP_ID ]; then 
    echo "APP_ID not set; exit..."
    exit -1
  fi
  if [ ! -e $JAR ]; then
    echo "JAR file not found; exit..."
    exit -1
  fi
}

run() {
  CONFIG=$TOPOLOGY_CONF,$METRICS_CONF
  storm-yarn jar -appId $APP_ID $JAR $MAIN_CLASS $TOPOLOGY_CLASS -c $CONFIG
}


kill_topology() {
  storm-yarn kill -appId $APP_ID $TOPOLOGY_NAME
}
