package storm.benchmark.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import storm.benchmark.topology.common.WordCount;
import storm.benchmark.util.KafkaUtils;
import storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;

public class KafkaWordCount extends WordCount {

  @Override
  public StormTopology getTopology(Config config) {
    spout = new KafkaSpout(KafkaUtils.getSpoutConfig(config, new SchemeAsMultiScheme(new StringScheme())));
    return super.getTopology(config);
  }
}
