package storm.benchmark;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import storm.benchmark.metrics.BasicMetricsCollector;
import storm.benchmark.metrics.IMetricsCollector;

public abstract class StormBenchmark implements IBenchmark {

  private static final Logger LOG = Logger.getLogger(StormBenchmark.class);
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
    collector.collect();
  }

  @Override
  public IMetricsCollector getMetricsCollector(Config config, StormTopology topology) {
    return new BasicMetricsCollector(config, topology);
  }

  public boolean ifAckEnabled(Config config) {
    Object ackers = config.get(Config.TOPOLOGY_ACKER_EXECUTORS);
    if (null == ackers) {
      throw new RuntimeException("acker executors are null");
    }
    return Utils.getInt(ackers) > 0;
  }
}
