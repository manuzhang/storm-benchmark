package storm.benchmark.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import org.testng.annotations.Test;
import storm.benchmark.StormBenchmark;
import storm.benchmark.util.TestUtils;

import static org.fest.assertions.api.Assertions.assertThat;

public class RollingCountTest {

  @Test
  public void componentParallelismCouldBeSetThroughConfig() {
    StormBenchmark benchmark = new RollingCount();
    Config config = new Config();
    config.put(RollingCount.SPOUT_NUM, 4);
    config.put(RollingCount.COUNTER_NUM, 3);

    StormTopology topology = benchmark.getTopology(config);
    assertThat(topology).isNotNull();
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, RollingCount.SPOUT_ID), 4);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, RollingCount.COUNTER_ID), 3);
  }
}
