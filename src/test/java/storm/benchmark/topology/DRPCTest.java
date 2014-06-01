package storm.benchmark.topology;

import backtype.storm.generated.StormTopology;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import storm.benchmark.StormBenchmark;

import java.util.HashMap;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;

public class DRPCTest {
  private final Map options = new HashMap();
  private StormBenchmark benchmark;

  @BeforeTest
  public void setUp() {
    benchmark = new DRPC();
    options.put(DRPC.SERVER, "localhost");
    options.put(DRPC.PORT, 10000);
    options.put(DRPC.SPOUT_NUM, 3);
    options.put(DRPC.PAGE_NUM, 4);
    options.put(DRPC.VIEW_NUM, 5);
  }

  @Test
  public void testBuildTopology() {
    benchmark.parseOptions(options).buildTopology();
    StormTopology topology = benchmark.getTopology();
    assertThat(topology).isNotNull();
 /*   TestUtils.verifyParallelism(Utils.getComponentCommon(topology, "spout0"), 3);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, "b-0"), 4);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, "b-1"), 5);*/
  }
}
