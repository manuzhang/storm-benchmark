Storm benchmark is a benchmark suite to measure the performanace of Storm. 

## How we measure storm performance

The benchmark consists 9 workloads. It falls into two categories. The first category is "simple resource benchmark", the goal is to how storm performs under pressure of certain resource. The second category is to measure how storm performs in real-life typical use cases.

 - Simple resource benchmarks:
    * wordcount, CPU sensitive
    * sol, network sensitive
    * rollingsort, memory sensitive

 - Real life use-case benchmark:
     * rollingcount
     * trident
     * uniquevisitor 
     * pageview
     * grep
     * dataclean
     * drpc

## How to use

1. Setup. 
   
  First, build storm-benchmark.
  ```bash
    git clone https://github.com/manuzhang/storm-benchmark.git
    mvn package
  ```
 Then copy the **./benchmark** directory and **stom-benchmark-with-dependencies.jar** onto a storm client node.

2. Config. modify `./benchmark/conf/config.sh`.

  
  ```bash
# Where is the storm binary
BIN=/usr/lib/storm/bin/storm

# Absolute path to the stom-benchmark-with-dependencies.jar
JAR=/root/storm-benchmark-0.1.0-jar-with-dependencies.jar

# Please don't modify this
MAIN_CLASS=storm.benchmark.tools.Runner

# We will pull the metrics from nimbus periodically. This defines the interval.
METRICS_POLL_INTERVAL=60000 # 60 secs

 # How long will we run for each benchmark.
METRICS_TOTAL_TIME=300000  # 5 mins

# Where we store the metrics reports. The metrics contains the performance and throughput information.
METRICS_PATH=/root/benchmark/reports
METRICS_CONF=metrics.time=$METRICS_TOTAL_TIME,metrics.poll=$METRICS_POLL_INTERVAL,metrics.path=$METRICS_PATH

# The default workers we use for the benchmark.
WORKERS=4

# The default ack tasks we use for the benchmark.
ACKERS=$WORKERS

# The default max.spout.pending(it will override the default storm config) we use for the benchmarks.
PENDING=200

### the kafka configuration
 # Kafka broker list [node1:port1, node2:port2, ...]
BROKER_LIST=intelidh-04:9092
# Zookeeper server list [node1:port1, node2:port2, ...]
ZOOKEEPER_SERVERS=intelidh-04:2181
# the root path you create for Kafka in Zookeeper
KAFKA_ROOT_PATH=/kafka/kafka-cluster-0  
```

3. Choose. Pick the benchmarks to run in `benchmark/conf/benchmark.lst`, for example, to run wordcount only

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

4. Run.  

  ```bash 
    benchmark/bin/run-all.sh
  ```

5. Check results.
 The benchmark results will be stored at config path METRICS_PATH(default is: /root/benchmark/reports). It contains througput data and latency of the whole cluster.
 
 The result contains two files:
   1. the benchmark performance data file. For wordcount, it is named like `WordCount_metrics_1402148415021.csv`
   2. The active config when running the benchmark. For wordcount, it is named like `WordCount_metrics_1402148415021.yaml`

Here is a snippet of what `WordCount_metrics_1402148415021.csv` looks like:


  ```
  time(s),total_slots,used_slots,workers,tasks,executors,transferred (messages),throughput (messages/s),spout_executors,spout_transferred (messages),spout_acked (messages),spout_throughput (messages/s),spout_avg_complete_latency(ms),spout_max_complete_latency(ms)
  60,16,4,4,24,24,37157180,619224,4,3807300,3806140,63448,11.5,11.7
  120,16,4,4,24,24,40878860,679479,4,4184340,7992280,69551,11.2,11.3
  ...
  ```

## Supports

Please contact:
Manu Zhang: tianlun.zhang@intel.com
Sean Zhong: xiang.zhong@intel.com

## Acknowledgement

We use the SOL benchmark code(https://github.com/yahoo/storm-perf-test) from yahoo. Thanks. 
