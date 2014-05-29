#!/bin/sh

APP_ID=application_1400224450350_0061
JAR=/root/storm-benchmark-0.9.0-Intel-jar-with-dependencies.jar
MAIN_CLASS=storm.benchmark.BenchmarkRunner

METRICS_POLL_INTERVAL=60000 # 60 secs
METRICS_TOTAL_TIME=300000  # 5 mins
METRICS_PATH=/root/benchmark/reports
METRICS_CONF=metrics.time=$METRICS_TOTAL_TIME,metrics.poll=$METRICS_POLL_INTERVAL,metrics.path=$METRICS_PATH


