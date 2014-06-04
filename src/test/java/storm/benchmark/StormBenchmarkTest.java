package storm.benchmark;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import storm.benchmark.metrics.IMetrics;
import storm.benchmark.metrics.StormMetrics;
import storm.benchmark.topology.FileReadWordCount;
import storm.benchmark.topology.RollingCount;
import storm.benchmark.topology.SOL;
import storm.benchmark.topology.TridentWordCount;

import java.util.HashMap;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;

public class StormBenchmarkTest {

  private static final Map ANY_MAP = new HashMap();

  @Test(dataProvider = "getBenchmarks")
  public void parseOptionsShouldCreateConfigAndMetrics(StormBenchmark benchmark, Class<IMetrics> metricsClass) {
    benchmark.parseOptions(ANY_MAP);
    assertThat(benchmark.getConfig())
            .isNotNull();
    assertThat(benchmark.getMetrics())
            .isNotNull()
            .isInstanceOf(metricsClass);

  }


  @DataProvider
  private Object[][] getBenchmarks() {
    return new Object[][] {
            { new FileReadWordCount(), StormMetrics.class },
            { new SOL(), StormMetrics.class },
            { new RollingCount(), StormMetrics.class },
            { new TridentWordCount(), StormMetrics.class }
    };
  }

}
