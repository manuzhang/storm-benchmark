package storm.benchmark;

import backtype.storm.StormSubmitter;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import storm.benchmark.metrics.IMetrics;
import storm.benchmark.metrics.StormMetrics;

import java.util.Map;

public abstract class StormBenchmark implements IBenchmark {

  protected BenchmarkConfig config;
  protected StormTopology topology;
  protected IMetrics metrics;

  @Override
  public IBenchmark parseOptions(Map options) {
    if (options != null) {
      config = new BenchmarkConfig(options);
    }
    metrics = new StormMetrics();
    return this;
  }

  @Override
  public IBenchmark submit() throws Exception {
    if (null == topology) {
      throw new InvalidTopologyException("no topology defined");
    }
    StormSubmitter.submitTopology(config.getTopologyName(), config.getStormConfig(), topology);
    return this;
  }

  @Override
  public IBenchmark startMetrics() {
    metrics.setConfig(config)
            .setTopology(topology)
            .start();
    return this;
  }

}
