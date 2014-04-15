package storm.benchmark.metrics;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import storm.benchmark.BenchmarkConfig;
import storm.benchmark.util.FileUtils;
import storm.benchmark.util.Util;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public abstract class StormMetrics implements IMetrics {

  public static final String METRICS_POLL_FREQ = "metrics.poll";
  public static final String METRICS_TOTAL_TIME = "metrics.time";

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
  public static final int THROUGHPUT_UNIT = 1000 * 1000; // MB
  public static final String ALL_TIME = ":all-time";
  public static final String LAST_TEN_MINS = "600";

  public static final String METRICS_CONF_FORMAT = "/root/%s_metrics_%d.conf";
  public static final String METRICS_FILE_FORMAT = "/root/%s_metrics_%d.csv";

  private static final Logger LOG = Logger.getLogger(StormMetrics.class);

  // How often should metrics be collected
  int poll = 30 * 1000; // 30 secs
  // How long should the benchmark run for
  int total = 5 * 60 * 1000; // 5 mins
  // message size
  int msgSize = 100; // 100 bytes

  BenchmarkConfig config;
  StormTopology topology;
  Map<String, SpoutSpec> spouts;
  Map<String, Bolt> bolts;
  Map<String, String> metrics = new HashMap<String, String>();
  PrintWriter confWriter = null;
  PrintWriter fileWriter = null;

  @Override
  public IMetrics setConfig(BenchmarkConfig config) {
    this.config = config;
    this.poll = Util.retIfPositive(this.poll, (Integer) this.config.getCommandLineOpts().get(METRICS_POLL_FREQ));
    this.total = Util.retIfPositive(this.total, (Integer) this.config.getCommandLineOpts().get(METRICS_TOTAL_TIME));
    try {
      final String topoName = (String) this.config.getTopologyName();
      final long time = System.currentTimeMillis();
      final String confName = String.format(METRICS_CONF_FORMAT, topoName, time);
      final String fileName = String.format(METRICS_FILE_FORMAT, topoName, time);
      this.confWriter = FileUtils.createFileWriter(confName);
      this.fileWriter = FileUtils.createFileWriter(fileName);
    } catch (IOException e) {
      LOG.error(e.getMessage());
      this.confWriter.close();
      this.fileWriter.close();
    }
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
    clusterConf.putAll(config.getStormConfig());
    return NimbusClient.getConfiguredClient(clusterConf).getClient();
  }

  public void writeOutCommandLineOpts(PrintWriter writer) {
    Map opts = config.getCommandLineOpts();
    if (writer != null) {
      for (Object key : opts.keySet()) {
        writer.println(key + "=" + opts.get(key));
      }
    }
  }

}
