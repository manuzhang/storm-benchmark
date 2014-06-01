#!/bin/bash

echo "========== running KafkaPageViewProducer =========="
CUR_DIR=`dirname "$0"`
BIN_DIR=$CUR_DIR/../../bin
CONF_DIR=$CUR_DIR/../../conf
. $CONF_DIR/config.sh
. $CUR_DIR/config.sh
. $BIN_DIR/functions.sh

check
run
