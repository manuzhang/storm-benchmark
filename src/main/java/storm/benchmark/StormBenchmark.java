package storm.benchmark;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import storm.benchmark.metrics.BasicMetricsCollector;
import storm.benchmark.metrics.IMetricsCollector;

import java.util.Set;

import static storm.benchmark.metrics.IMetricsCollector.MetricsItem;

public abstract class StormBenchmark implements IBenchmark {

  private static final Logger LOG = Logger.getLogger(StormBenchmark.class);
  public static final String DEFAULT_TOPOLOGY_NAME = "benchmark";
  protected String name;
  protected StormTopology topology;
  protected IMetricsCollector collector;

  @Override
  public void run() throws Exception {
    Config config = new Config();
    config.putAll(Utils.readStormConfig());
    name = (String) config.get(Config.TOPOLOGY_NAME);
    topology = getTopology(config);
    StormSubmitter.submitTopology(name, config, topology);
    collector = getMetricsCollector(config, topology);
    collector.run();
  }

  @Override
  public IMetricsCollector getMetricsCollector(Config config, StormTopology topology) {

    Set<MetricsItem> items = Sets.newHashSet(
            MetricsItem.SUPERVISOR_STATS,
            MetricsItem.TOPOLOGY_STATS,
            MetricsItem.THROUGHPUT,
            MetricsItem.SPOUT_THROUGHPUT,
            MetricsItem.SPOUT_LATENCY
            );
    return new BasicMetricsCollector(config, topology, items);
  }

  public boolean ifAckEnabled(Config config) {
    Object ackers = config.get(Config.TOPOLOGY_ACKER_EXECUTORS);
    if (null == ackers) {
      LOG.warn("acker executors are null");
      return false;
    }
    return Utils.getInt(ackers) > 0;
  }
}
