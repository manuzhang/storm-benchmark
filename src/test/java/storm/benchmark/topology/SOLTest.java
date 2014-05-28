package storm.benchmark.topology;

import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import storm.benchmark.metrics.StormMetrics;
import storm.benchmark.util.TestUtils;

import java.util.HashMap;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;

public class SOLTest {
  private final Map ANY_MAP = new HashMap();
  private final Map options = new HashMap();
  private SOL benchmark;

  @BeforeTest
  public void init() {
    benchmark = new SOL();
    options.put(SOL.SPOUT, 4);
    options.put(SOL.BOLT, 3);
    options.put(SOL.TOPOLOGY_LEVEL, 3);
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
    benchmark.parseOptions(options).buildTopology();
    StormTopology topology = benchmark.getTopology();
    assertThat(topology).isNotNull();
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, SOL.SPOUT), 4);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, SOL.BOLT + "1"), 3);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, SOL.BOLT + "2"), 3);

  }
}
