package storm.benchmark.metrics;

import backtype.storm.generated.StormTopology;
import storm.benchmark.BenchmarkConfig;

public interface IMetrics {
  public IMetrics start();
  public IMetrics setConfig(BenchmarkConfig config);
  public IMetrics setTopology(StormTopology topology);
}
