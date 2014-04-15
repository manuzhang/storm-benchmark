package storm.benchmark.metrics;

public class TridentMetrics extends StormMetrics {
  @Override
  public IMetrics start() {
    return this;
  }
}
