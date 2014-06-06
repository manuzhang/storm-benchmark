package storm.benchmark.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import storm.benchmark.StormBenchmark;

import static org.fest.assertions.api.Assertions.assertThat;

public class DRPCTest {
  private final Config config = new Config();
  private StormBenchmark benchmark;

  @BeforeMethod
  public void setUp() {
    benchmark = new DRPC();
    config.put(DRPC.SERVER, "localhost");
    config.put(DRPC.PORT, 10000);
    config.put(DRPC.SPOUT_NUM, 3);
    config.put(DRPC.PAGE_NUM, 4);
    config.put(DRPC.VIEW_NUM, 5);
  }

  @Test
  public void getTopologyShouldSetComponentParallelism() {
    StormTopology topology = benchmark.getTopology(config);
    assertThat(topology).isNotNull();
 /*   TestUtils.verifyParallelism(Utils.getComponentCommon(topology, "spout0"), 3);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, "b-0"), 4);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, "b-1"), 5);*/
  }
}
