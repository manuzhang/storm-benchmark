package storm.benchmark;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import storm.benchmark.metrics.IMetricsCollector;

/**
 * Interface for all Benchmark topologies
 * A benchmark class should implement the following methods
 * which would be serially carried out by BenchmarkRunner
 */
public interface IBenchmark {
  public StormTopology getTopology(Config config);
  public IMetricsCollector getMetricsCollector(Config config, StormTopology topology);
  public void run() throws Exception;
}
