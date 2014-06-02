#!/bin/bash

echo "========== running UniqueVisitor =========="
CUR_DIR=`dirname "$0"`
BIN_DIR=$CUR_DIR/../bin
CONF_DIR=$CUR_DIR/../conf
PRODUCER_DIR=$CUR_DIR/../producer/pageview
. $CONF_DIR/config.sh
. $CUR_DIR/config.sh
. $BIN_DIR/functions.sh

run_producer
run_benchmark
kill_producer
