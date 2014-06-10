# Introduction

Storm benchmark is a benchmark suite to measure the performanace of Storm. 


## Workloads

Currently, the benchmark consists 9 workloads. 

* wordcount 
* sol
* rollingcount
* trident
* uniquevisitor 
* pageview
* grep
* dataclean
* drpc

They fall into two groups. The first group (wordcount, sol, rollingsort) are meant to measure Storm from resource utilization perspective and have no external dependencies. The second group (the rest) are of real use cases and need Kafka as a source.

## How to build 

1. get the source codes

  ```bash
    git clone https://github.com/manuzhang/storm-benchmark.git
  ```

2. build with maven

  ```bash
    mvn install -DskipTests
  ```

## How to run

1. copy the **benchmark** directory and **stom-benchmark-with-dependencies.jar** onto a storm client node.


2. modify `benchmark/conf/config.sh` according to your own setups. 

  a. basic conf

  The following values are required to run a storm job. 
  
  
  ```bash
    JAR          # Absolute path to the stom-benchmark-with-dependencies.jar
    MAIN_CLASS   # Please don't modify this
  ```

  b. metrics conf
  
  ```bash
    METRICS_POLL_INTERVAL   # the interval to poll metrics data 
    METRICS_TOTAL_TIME      # the total time of collecting metrics
    METRICS_PATH            # the path to metrics reports
  ```
  
  c. Kafka conf
  
  The values are required for any jobs needing Kafka
  
  ```bash
    BROKER_LIST         # Kafka broker list [node1:port1, node2:port2, ...]
    ZOOKEEPER_SERVERS   # Zookeeper server list [node1:port1, node2:port2, ...]
    KAFKA_ROOT_PATH     # the root path you create for Kafka in Zookeeper
  ```      

3. select the topologies to run

  The workloads are listed in `benchmark/conf/benchmark.lst`, and by default all of them would be run 
  in that order. You can leave out a workload by prepanding a "#".
  For example, to run wordcount only

  ```bash
    wordcount
    #sol
    #rollingcount
    #trident
    #uniquevisitor
    #pageview
    #grep
    #dataclean
    #drpc
  ```


4. run  

  ```bash 
    benchmark/bin/run-all.sh
  ```


## Metrics 

The benchmark would collect one or more of the following metrics for a workload.

* supervisor stats (used slots / total slots)
* topology stats (# of workers/executors/tasks)
* throughput 
* spout throughput 
* spout latency 
* end to end latency for DRPC 

All metrics data are written to the directory configured via `METRICS_PATH`. For each workload, there is a yaml file with all the configurations and csv file with the metrics data. File names follow the pattern `${WORKLOAD}_metrics_${TIMESTAMP}.*`. Take Wordcount for instance, if setting `METRICS_PATH=/root/benchmark/reports`, I will have `WordCount_metrics_1402148415021.csv` and `WordCount_metrics_1402148415021.yaml` under `/root/benchmark/reports` after running the benchmark. If everything goes fine, the metrics data file looks like this


  ```
  time(s),total_slots,used_slots,workers,tasks,executors,transferred (messages),throughput (messages/s),spout_executors,spout_transferred (messages),spout_acked (messages),spout_throughput (messages/s),spout_avg_complete_latency(ms),spout_max_complete_latency(ms)
  60,16,4,4,24,24,37157180,619224,4,3807300,3806140,63448,11.5,11.7
  120,16,4,4,24,24,40878860,679479,4,4184340,7992280,69551,11.2,11.3
  180,16,4,4,24,24,41157280,685292,4,4215800,12207840,70195,11.3,11.4
  240,16,4,4,24,24,43499640,724281,4,4451200,16663480,74113,11.1,11.3
  300,16,4,4,24,24,41669280,693875,4,4264000,20931440,71003,11.1,11.2
  ```

