package storm.benchmark;

import backtype.storm.Config;
import org.apache.log4j.Logger;
import storm.benchmark.util.BenchmarkUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * BenchmarkConfig is a wrapper over Storm config
 * It provides default values to config if not set by user
 */
public class BenchmarkConfig {
  public static final String METRICS_CONSUMER = "metrics.consumer";
  public static final String METRICS_CONSUMER_PARALLELISM = "metrics.consumer.parallelism";
  public static final String MESSAGE_SIZE = "message.size";
  // disable debug
  public static final boolean DISABLE_DEBUG = true;
 // base name of the topology
  public static final String DEFAULT_TOPOLOGY_NAME = "test";
  // number of workers
  public static final int DEFAULT_WORKERS = 4;
  // number of ackers
  public static final int DEFAULT_ACKERS = DEFAULT_WORKERS;

  // default size for executer receive buffer
  public static final int DEFAULT_ERBSIZE = 16384;
  // default size for executor send buffer
  public static final int DEFAULT_ESBSIZE = 16384;
  // default size for worker receive buffer
  public static final int DEFAULT_WRBSIZE = 8;
  // default size for worker transfer buffer
  public static final int DEFAULT_WTBSIZE = 32;

  private final Config config = new Config();

  private static final Logger LOG = Logger.getLogger(BenchmarkConfig.class);

  public BenchmarkConfig() {
    this(new HashMap());
  }


  public BenchmarkConfig(Map options) {
    config.putAll(checkAndSetDefault(options));
  }

  public Config getStormConfig() {
    return config;
  }

  public String getTopologyName() {
    return (String) config.get(Config.TOPOLOGY_NAME);
  }

  public Boolean ifAckEnabled() {
    return (Integer) config.get(Config.TOPOLOGY_ACKER_EXECUTORS) > 0;
  }

  private Map checkAndSetDefault(Map options) {
    if (null == options) {
      throw new IllegalArgumentException("no storm config available");
    }

    BenchmarkUtils.putIfAbsent(options, Config.TOPOLOGY_NAME, DEFAULT_TOPOLOGY_NAME);
    BenchmarkUtils.putIfAbsent(options, Config.TOPOLOGY_DEBUG, DISABLE_DEBUG);
    BenchmarkUtils.putIfAbsent(options, Config.TOPOLOGY_WORKERS, DEFAULT_WORKERS);
    BenchmarkUtils.putIfAbsent(options, Config.TOPOLOGY_ACKER_EXECUTORS, DEFAULT_ACKERS);
    BenchmarkUtils.putIfAbsent(options, Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, DEFAULT_ERBSIZE);
    BenchmarkUtils.putIfAbsent(options, Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, DEFAULT_ESBSIZE);
    BenchmarkUtils.putIfAbsent(options, Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, DEFAULT_WRBSIZE);
    BenchmarkUtils.putIfAbsent(options, Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, DEFAULT_WTBSIZE);


    if (options.containsKey(METRICS_CONSUMER)) {
      try {
        Class consumer =
              Class.forName((String) options.get(METRICS_CONSUMER));
        int parallel = BenchmarkUtils.getInt(options, METRICS_CONSUMER_PARALLELISM, 1);
        config.registerMetricsConsumer(consumer, parallel);
      } catch (ClassNotFoundException e) {
        LOG.error(e.getMessage());
      }
    }
    return options;
  }
}
