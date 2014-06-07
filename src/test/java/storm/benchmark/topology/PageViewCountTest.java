package storm.benchmark.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import org.testng.annotations.Test;
import storm.benchmark.StormBenchmark;
import storm.benchmark.util.TestUtils;

import static org.fest.assertions.api.Assertions.assertThat;

public class PageViewCountTest {

  @Test
  public void componentParallelismCouldBeSetThroughConfig() {
    StormBenchmark benchmark = new PageViewCount();
    Config config = new Config();
    config.put(PageViewCount.SPOUT_NUM, 3);
    config.put(PageViewCount.VIEW_NUM, 4);
    config.put(PageViewCount.COUNT_NUM, 5);

    StormTopology topology = benchmark.getTopology(config);
    assertThat(topology).isNotNull();
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, PageViewCount.SPOUT_ID), 3);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, PageViewCount.VIEW_ID), 4);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, PageViewCount.COUNT_ID), 5);
  }
}
