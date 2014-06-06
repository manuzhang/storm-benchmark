package storm.benchmark.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import org.testng.annotations.Test;
import storm.benchmark.StormBenchmark;
import storm.benchmark.topology.common.WordCount;
import storm.benchmark.util.TestUtils;

import static org.fest.assertions.api.Assertions.assertThat;

public class FileReadWordCountTest {

  @Test
  public void componentParallelismCouldBeSetThroughConfig() {
    StormBenchmark benchmark = new FileReadWordCount();
    Config config = new Config();
    config.put(WordCount.SPOUT_NUM, 3);
    config.put(WordCount.SPLIT_NUM, 4);
    config.put(WordCount.COUNT_NUM, 5);
    StormTopology topology = benchmark.getTopology(config);
    assertThat(topology).isNotNull();
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, WordCount.SPOUT_ID), 3);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, WordCount.SPLIT_ID), 4);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, WordCount.COUNT_ID), 5);
  }


}
