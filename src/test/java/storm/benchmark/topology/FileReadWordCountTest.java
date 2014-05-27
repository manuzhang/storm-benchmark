package storm.benchmark.topology;

import org.testng.annotations.Test;
import storm.benchmark.metrics.StormMetrics;
import storm.benchmark.spout.FileReadSpout;

import java.util.HashMap;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;

public class FileReadWordCountTest {

  public static final Map ANY_MAP = new HashMap();

  @Test
  public void testParseOptions() {
    FileReadWordCount benchmark = new FileReadWordCount();
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
  public void testBuildToplogy() {
    FileReadWordCount benchmark = new FileReadWordCount();
    benchmark.parseOptions(ANY_MAP).buildTopology();
    assertThat(benchmark.getTopology()).isNotNull();
  }
}
