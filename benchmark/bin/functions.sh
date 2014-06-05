#!/bin/sh 

run_benchmark() {
  check
  run_topology
  kill_topology
}


check() {
  if [ ! -e $JAR ]; then
    echo "JAR file not found; exit..."
    exit -1
  fi
}

run_topology() {
  CONFIG=$TOPOLOGY_CONF,$METRICS_CONF,$KAFKA_CONF
  CONFIG=`echo $CONFIG | sed -r "s/,/ -c /g"`
  $BIN jar $JAR $MAIN_CLASS $TOPOLOGY_CLASS $CONFIG
}


kill_topology() {
  $BIN kill $TOPOLOGY_NAME
}

run_producer() {
  sh $PRODUCER_DIR/run.sh
}

kill_producer() {
  $BIN kill $PRODUCER_NAME
}

