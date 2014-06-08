# Introduction

Storm benchmark is a benchmark suite to measure the performanace of Storm-on-YARN. 


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
    git clone -b storm-intel https://github.com/manuzhang/storm-benchmark.git
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




