package storm.benchmark;

import backtype.storm.Config;
import org.apache.log4j.Logger;
import storm.benchmark.util.Util;

import java.util.HashMap;
import java.util.Map;

public class BenchmarkConfig {
  public static final String METRICS_CONSUMER = "metrics.consumer";
  public static final String METRICS_CONSUMER_PARALLELISM = "metrics.consumer.parallelism";

  // enable debug
  public static final boolean ENABLE_DEBUG = false;
 // base name of the topology
  public static final String DEFAULT_TOPOLOGY_NAME = "test";
  // number of workers
  public static final int DEFAULT_WORKERS = 4;
  // number of ackers
  public static final int DEFAULT_EXECUTORS = DEFAULT_WORKERS;

  // default size for executer receive buffer
  public static final int DEFAULT_ERBSIZE = 16384;
  // default size for executor send buffer
  public static final int DEFAULT_ESBSIZE = 16384;
  // default size for worker receive buffer
  public static final int DEFAULT_WRBSIZE = 8;
  // default size for worker transfer buffer
  public static final int DEFAULT_WTBSIZE = 32;

  private Config config = new Config();
  private Map options = new HashMap();

  private static final Logger LOG = Logger.getLogger(BenchmarkConfig.class);

  public BenchmarkConfig(Map options) {
    this(new Config(), options);
  }

  public BenchmarkConfig(Config  config, Map options) {
    this.config = config;
    this.options = options;
    checkAndSetDefault(options);
  }

  public Config getStormConfig() {
    return config;
  }

  public Map getCommandLineOpts() {
    return options;
  }

  public String getTopologyName() {
    return (String) config.get(Config.TOPOLOGY_NAME);
  }

  public Boolean ifAckEnabled() {
    return (Integer)config.get(Config.TOPOLOGY_ACKER_EXECUTORS) > 0;
  }

  private void checkAndSetDefault(Map options) {
    Util.putIfAbsent(options, Config.TOPOLOGY_NAME, DEFAULT_TOPOLOGY_NAME);
    Util.putIfAbsent(options, Config.TOPOLOGY_DEBUG, ENABLE_DEBUG);
    Util.putIfAbsent(options, Config.TOPOLOGY_WORKERS, DEFAULT_WORKERS);
    Util.putIfAbsent(options, Config.TOPOLOGY_ACKER_EXECUTORS, DEFAULT_EXECUTORS);
    Util.putIfAbsent(options, Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, DEFAULT_ERBSIZE);
    Util.putIfAbsent(options, Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, DEFAULT_ESBSIZE);
    Util.putIfAbsent(options, Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, DEFAULT_WRBSIZE);
    Util.putIfAbsent(options, Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, DEFAULT_WTBSIZE);


    if (options.containsKey(METRICS_CONSUMER)) {
      try {
        Class consumer =
              Class.forName((String) options.remove(METRICS_CONSUMER));
        int parallel = Util.retIfPositive(1,
                (Integer) options.remove(METRICS_CONSUMER_PARALLELISM));
        config.registerMetricsConsumer(consumer, parallel);
      } catch (ClassNotFoundException e) {
        LOG.error(e.getMessage());
      }
    }

    config.putAll(options);
  }
}
