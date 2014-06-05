package storm.benchmark.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import storm.benchmark.StormBenchmark;
import storm.benchmark.util.TestUtils;

import static org.fest.assertions.api.Assertions.assertThat;

public class SOLTest {
  private final Config config = new Config();
  private StormBenchmark benchmark;

  @BeforeTest
  public void setUp() {
    benchmark = new SOL();
    config.put(SOL.SPOUT_NUM, 4);
    config.put(SOL.BOLT_NUM, 3);
    config.put(SOL.TOPOLOGY_LEVEL, 3);
  }

  @Test
  public void testBuildToplogy() {
    StormTopology topology = benchmark.getTopology(config);
    assertThat(topology).isNotNull();
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, SOL.SPOUT_ID), 4);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, SOL.BOLT_ID + "1"), 3);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, SOL.BOLT_ID + "2"), 3);
  }
}
