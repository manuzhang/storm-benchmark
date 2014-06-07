package storm.benchmark.metrics;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import storm.benchmark.StormBenchmark;
import storm.benchmark.util.BenchmarkUtils;

import java.io.PrintWriter;
import java.util.Map;
import java.util.TreeMap;

public class MetricsCollectorConfig {
  private static final Logger LOG = Logger.getLogger(MetricsCollectorConfig.class);

  public static final String CONF_FILE_FORMAT = "%s/%s_metrics_%d.yaml";
  public static final String DATA_FILE_FORMAT = "%s/%s_metrics_%d.csv";

  public static final String METRICS_POLL_INTERVAL = "metrics.poll";
  public static final String METRICS_TOTAL_TIME = "metrics.time";
  public static final String METRICS_PATH = "metrics.path";

  public static final int DEFAULT_POLL_INTERVAL = 30 * 1000; // 30 secs
  public static final int DEFAULT_TOTAL_TIME = 5 * 60 * 1000; // 5 mins
  public static final String DEFAULT_PATH = "/root/";

  // storm configuration
  public final Config stormConfig;
  // storm topology name
  public final String name;
  // How often should metrics be collected
  public final int pollInterval;
  // How long should the benchmark run for
  public final int totalTime;
  // metrics file path
  public final String path;

  public MetricsCollectorConfig(Config stormConfig) {
    this.stormConfig  = stormConfig;
    name = (String) Utils.get(
            stormConfig, Config.TOPOLOGY_NAME, StormBenchmark.DEFAULT_TOPOLOGY_NAME);
    pollInterval = BenchmarkUtils.getInt(
            stormConfig, METRICS_POLL_INTERVAL, DEFAULT_POLL_INTERVAL);
    totalTime = BenchmarkUtils.getInt(
            stormConfig, METRICS_TOTAL_TIME, DEFAULT_TOTAL_TIME);
    path = (String) Utils.get(stormConfig, METRICS_PATH, DEFAULT_PATH);
  }


  public void writeStormConfig(PrintWriter writer) {
    LOG.info("writing out storm config into .yaml file");
    if (writer != null) {
      Map sorted = new TreeMap();
      sorted.putAll(stormConfig);
      for (Object key : sorted.keySet()) {
        writer.println(key + ": " + stormConfig.get(key));
      }
      writer.flush();
    }
  }
}
