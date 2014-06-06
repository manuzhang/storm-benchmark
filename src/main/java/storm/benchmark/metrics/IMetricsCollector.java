package storm.benchmark.metrics;

public interface IMetricsCollector {
  public static enum MetricsItem {
    SUPERVISOR_STATS,
    TOPOLOGY_STATS,
    THROUGHPUT,
    SPOUT_THROUGHPUT,
    SPOUT_LATENCY,
    THROUGHPUT_IN_MB,
    ALL
  }
  public void run();
}
