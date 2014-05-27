package storm.benchmark.topology;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import storm.benchmark.metrics.StormMetrics;

import java.util.HashMap;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;

public class SOLTest {
  private Map ANY_MAP = new HashMap();
  private SOL benchmark;

  @BeforeTest
  public void init() {
    benchmark = new SOL();
  }

  @Test
  public void testParseOptions() {
    benchmark.parseOptions(ANY_MAP);
    assertThat(benchmark.getConfig()).isNotNull();
    assertThat(benchmark.getMetrics())
            .isNotNull()
            .isInstanceOf(StormMetrics.class);
  }

  @Test
  public void testBuildToplogy() {
    benchmark.parseOptions(ANY_MAP).buildTopology();
    assertThat(benchmark.getTopology()).isNotNull();
  }
}
