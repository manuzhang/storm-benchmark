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

public class TridentWordCountTest {
  private final Map options = new HashMap();
  private StormBenchmark benchmark;

  @BeforeTest
  public void setUp() {
    benchmark = new TridentWordCount();
    options.put(TridentWordCount.SPOUT_NUM, 3);
    options.put(TridentWordCount.SPLIT_NUM, 4);
    options.put(TridentWordCount.COUNT_NUM, 5);
  }

  @Test
  public void testBuildTopology() {
    benchmark.parseOptions(options).buildTopology();
    StormTopology topology = benchmark.getTopology();
    assertThat(topology).isNotNull();
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, "spout0"), 3);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, "b-0"), 4);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, "b-1"), 5);
  }
}
