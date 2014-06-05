package storm.benchmark.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import storm.benchmark.component.spout.FileReadSpout;
import storm.benchmark.topology.common.WordCount;

public class FileReadWordCount extends WordCount {

  @Override
  public StormTopology getTopology(Config config) {
    spout = new FileReadSpout(ifAckEnabled(config));
    return super.getTopology(config);
  }
}
