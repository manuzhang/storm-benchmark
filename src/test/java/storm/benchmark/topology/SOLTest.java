package storm.benchmark.topology;

import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import storm.benchmark.StormBenchmark;
import storm.benchmark.util.TestUtils;

import java.util.HashMap;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;

public class SOLTest {
  private final Map ANY_MAP = new HashMap();
  private final Map options = new HashMap();
  private StormBenchmark benchmark;

  @BeforeTest
  public void setUp() {
    benchmark = new SOL();
    options.put(SOL.SPOUT_NUM, 4);
    options.put(SOL.BOLT_NUM, 3);
    options.put(SOL.TOPOLOGY_LEVEL, 3);
  }

  @Test
  public void testBuildToplogy() {
    benchmark.parseOptions(options).buildTopology();
    StormTopology topology = benchmark.getTopology();
    assertThat(topology).isNotNull();
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, SOL.SPOUT_ID), 4);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, SOL.BOLT_ID + "1"), 3);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, SOL.BOLT_ID + "2"), 3);

  }
}
