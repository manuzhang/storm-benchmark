package storm.benchmark.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import org.testng.annotations.Test;
import storm.benchmark.StormBenchmark;
import storm.benchmark.util.TestUtils;

import static org.fest.assertions.api.Assertions.assertThat;

public class UniqueVisitorTest {

  @Test
  public void componentParallelismCouldBeSetThroughConfig() {
    StormBenchmark benchmark = new UniqueVisitor();
    Config config = new Config();
    config.put(UniqueVisitor.SPOUT_NUM, 3);
    config.put(UniqueVisitor.VIEW_NUM, 4);
    config.put(UniqueVisitor.UNIQUER_NUM, 5);
    StormTopology topology = benchmark.getTopology(config);
    assertThat(topology).isNotNull();
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, UniqueVisitor.SPOUT_ID), 3);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, UniqueVisitor.VIEW_ID), 4);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, UniqueVisitor.UNIQUER_ID), 5);
  }
}
