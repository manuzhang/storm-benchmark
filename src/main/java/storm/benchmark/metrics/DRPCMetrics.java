package storm.benchmark.metrics;


/*
 * DRPCMetrics is meant to collect end-to-end latency for DRPC benchmarks
 * the latency is the time between DRPCClient submitting a query and
 * receiving the result
 */

public class DRPCMetrics extends StormMetrics {
  @Override
  public IMetrics start() {
    return this;
  }
}
