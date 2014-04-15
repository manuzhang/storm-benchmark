package storm.benchmark;

import java.util.Map;

public interface IBenchmark {
  public IBenchmark parseOptions(Map options);
  public IBenchmark buildTopology();
  public IBenchmark submit() throws Exception;
  public IBenchmark startMetrics();
}
