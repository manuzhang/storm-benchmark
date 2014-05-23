package storm.benchmark.metrics;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import storm.benchmark.BenchmarkConfig;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.FileUtils;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public abstract class StormMetrics implements IMetrics {

  public static final String METRICS_POLL_FREQ = "metrics.poll";
  public static final String METRICS_TOTAL_TIME = "metrics.time";
  public static final String METRICS_PATH = "metrics.path";

  /* headers */
  public static final String TIME = "time(s)";
  public static final String TIME_FORMAT = "%d";
  public static final String TOTAl_SLOTS = "total_slots";
  public static final String USED_SLOTS = "used_slots";
  public static final String WORKERS = "workers";
  public static final String TASKS = "tasks";
  public static final String EXECUTORS = "total_executors";
  public static final String EXECUTORS_METRICS = "executors_with_metrics";
  public static final String TRANSFERRED = "overall_transferred (messages)";
  public static final String THROUGHPUT = "overall_throughput (messages/s)";
  public static final String THROUGHPUT_MB = "overall_throughput (MB/s)";
  public static final String THROUGHPUT_MB_FORMAT = "%.1f";
  public static final String SPOUT_EXECUTORS = "spout_executors";
  public static final String SPOUT_TRANSFERRED = "spout_transferred (messages)";
  public static final String SPOUT_ACKED = "spout_acked (messages)";
  public static final String SPOUT_THROUGHPUT = "spout_throughput (messages/s)";
  public static final String SPOUT_THROUGHPUT_MB = "spout_throughput (MB/s)";
  public static final String SPOUT_THROUGHPUT_MB_FORMAT = "%.3f";


  public static final String ALL_TIME = ":all-time";
  public static final String LAST_TEN_MINS = "600";
  public static final String LAST_THREE_HOURS = "10800";
  public static final String LAST_DAY = "86400";

  public static final String METRICS_CONF_FORMAT = "%s/%s_metrics_%d.conf";
  public static final String METRICS_FILE_FORMAT = "%s/%s_metrics_%d.csv";

  private static final Logger LOG = Logger.getLogger(StormMetrics.class);

  // How often should metrics be collected
  int poll = 30 * 1000; // 30 secs
  // How long should the benchmark run for
  int total = 5 * 60 * 1000; // 5 mins
  // message size
  int msgSize = 100; // 100 bytes
  // default path for metrics files
  String path = "/root/";

  Config config;
  StormTopology topology;
  String topoName;
  Map<String, SpoutSpec> spouts;
  Map<String, Bolt> bolts;
  Map<String, String> metrics = new HashMap<String, String>();
  PrintWriter confWriter = null;
  PrintWriter fileWriter = null;

  @Override
  public IMetrics setConfig(BenchmarkConfig benchConfig) {
    this.config = benchConfig.getStormConfig();
    poll = BenchmarkUtils.getInt(config, METRICS_POLL_FREQ, poll);
    total = BenchmarkUtils.getInt(config, METRICS_TOTAL_TIME, total);
    msgSize = BenchmarkUtils.getInt(config, BenchmarkConfig.MESSAGE_SIZE, msgSize);
    path = (String) Utils.get(config, METRICS_PATH, path);
    topoName = benchConfig.getTopologyName();
    final long time = System.currentTimeMillis();
    final String confName = String.format(METRICS_CONF_FORMAT, this.path, topoName, time);
    final String fileName = String.format(METRICS_FILE_FORMAT, this.path, topoName, time);
    this.confWriter = FileUtils.createFileWriter(confName);
    this.fileWriter = FileUtils.createFileWriter(fileName);
    return this;
  }

  @Override
  public IMetrics setTopology(StormTopology topology) {
    this.topology = topology;
    this.spouts = this.topology.get_spouts();
    this.bolts = this.topology.get_bolts();
    return this;
  }

  public Nimbus.Client getNimbusClient() {
    Map clusterConf = Utils.readStormConfig();
    clusterConf.putAll(config);
    return NimbusClient.getConfiguredClient(clusterConf).getClient();
  }

  public void writeOutStormConfig(PrintWriter writer) {
    if (writer != null) {
      for (Object key : config.keySet()) {
        writer.println(key + "=" + config.get(key));
      }
    }
  }

}
