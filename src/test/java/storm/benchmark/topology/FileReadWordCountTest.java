package storm.benchmark.topology;

import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import storm.benchmark.metrics.StormMetrics;
import storm.benchmark.spout.FileReadSpout;
import storm.benchmark.topology.common.WordCount;
import storm.benchmark.util.TestUtils;

import java.util.HashMap;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;

public class FileReadWordCountTest {

  private final Map ANY_MAP = new HashMap();
  private final Map options = new HashMap();
  private FileReadWordCount benchmark;

  @BeforeTest
  public void init() {
    benchmark = new FileReadWordCount();
    options.put(WordCount.SPOUT, 3);
    options.put(WordCount.SPLIT, 4);
    options.put(WordCount.COUNT, 5);
  }

  @Test
  public void testParseOptions() {
    benchmark.parseOptions(ANY_MAP);
    assertThat(benchmark.getSpout())
            .isNotNull()
            .isInstanceOf(FileReadSpout.class);
    assertThat(benchmark.getConfig()).isNotNull();
    assertThat(benchmark.getMetrics())
            .isNotNull()
            .isInstanceOf(StormMetrics.class);
  }

  @Test
  public void testBuildTopology() {
    benchmark.parseOptions(options).buildTopology();
    StormTopology topology = benchmark.getTopology();
    assertThat(topology).isNotNull();
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, WordCount.SPOUT), 3);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, WordCount.SPLIT), 4);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, WordCount.COUNT), 5);
  }


}
