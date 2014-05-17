package storm.benchmark.topology;

import storm.benchmark.IBenchmark;
import storm.benchmark.spout.FileReadSpout;
import storm.benchmark.topology.common.WordCount;

import java.util.Map;

public class FileReadWordCount extends WordCount {

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    spout = new FileReadSpout(config.ifAckEnabled());
    return this;
  }
}
