package storm.benchmark.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import storm.benchmark.StormBenchmark;
import storm.benchmark.util.TestUtils;

import static org.fest.assertions.api.Assertions.assertThat;

public class TridentWordCountTest {
  private final Config config = new Config();
  private StormBenchmark benchmark;

  @BeforeTest
  public void setUp() {
    benchmark = new TridentWordCount();
    config.put(TridentWordCount.SPOUT_NUM, 3);
    config.put(TridentWordCount.SPLIT_NUM, 4);
    config.put(TridentWordCount.COUNT_NUM, 5);
  }

  @Test
  public void testBuildTopology() {
    StormTopology topology = benchmark.getTopology(config);
    assertThat(topology).isNotNull();
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, "spout0"), 3);
  //  TestUtils.verifyParallelism(Utils.getComponentCommon(topology, "b-0"), 4);
  //  TestUtils.verifyParallelism(Utils.getComponentCommon(topology, "b-1"), 5);
  }
}
