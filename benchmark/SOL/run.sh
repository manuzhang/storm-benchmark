#!/bin/bash

echo "========== running SOL =========="
CUR_DIR=`dirname "$0"`
BIN_DIR=$CUR_DIR/../bin
CONF_DIR=$CUR_DIR/../conf
. $CONF_DIR/config.sh
. $DIR/config.sh
. $BIN_DIR/functions.sh

run_benchmark

