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

public class GrepTest {
  private final Map options = new HashMap();
  private StormBenchmark benchmark;

  @BeforeTest
  public void setUp() {
    benchmark = new Grep();
    options.put(Grep.SPOUT_NUM, 3);
    options.put(Grep.FM_NUM, 4);
    options.put(Grep.CM_NUM, 5);
  }

  @Test
  public void testBuildTopology() {
    benchmark.parseOptions(options).buildTopology();
    StormTopology topology = benchmark.getTopology();
    assertThat(topology).isNotNull();
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, Grep.SPOUT_ID), 3);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, Grep.FM_ID), 4);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, Grep.CM_ID), 5);
  }
}
