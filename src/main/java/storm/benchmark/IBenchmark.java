package storm.benchmark;

import java.util.Map;

/**
 * Interface for all Benchmark topologies
 * A benchmark class should implement the following methods
 * which would be serially carried out by BenchmarkRunner
 */
public interface IBenchmark {
  /**
   *
   * @param options storm.yaml + command line (set with "-c")
   * @return
   */
  public IBenchmark parseOptions(Map options);
  public IBenchmark buildTopology();
  public IBenchmark submit() throws Exception;
  public IBenchmark startMetrics();
}
