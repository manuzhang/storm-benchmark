package storm.benchmark;

import backtype.storm.StormSubmitter;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import org.apache.log4j.Logger;
import storm.benchmark.metrics.IMetrics;
import storm.benchmark.metrics.StormMetrics;

import java.util.Map;

public abstract class StormBenchmark implements IBenchmark {

  private static final Logger LOG = Logger.getLogger(StormBenchmark.class);
  protected BenchmarkConfig config;
  protected StormTopology topology;
  protected IMetrics metrics;

  @Override
  public IBenchmark parseOptions(Map options) {
    if (options != null) {
      if (options.isEmpty()) {
        LOG.warn("options are empty");
      }
      config = new BenchmarkConfig(options);
    } else {
      throw new IllegalArgumentException("options are null");
    }
    metrics = new StormMetrics();
    return this;
  }

  @Override
  public IBenchmark submit() throws Exception {
    if (null == topology) {
      throw new InvalidTopologyException("no topology defined");
    }
    String topoName = config.getTopologyName();
    LOG.info("submitting topology: " + topoName);
    StormSubmitter.submitTopology(topoName, config.getStormConfig(), topology);
    return this;
  }

  @Override
  public IBenchmark startMetrics() {
    LOG.info("starting metrics...");
    metrics.setConfig(config)
            .setTopology(topology)
            .start();
    return this;
  }

  public BenchmarkConfig getConfig() {
    return config;
  }

  public StormTopology getTopology() {
    return topology;
  }

  public IMetrics getMetrics() {
    return metrics;
  }

}
