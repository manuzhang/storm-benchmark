package storm.benchmark.topology;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.benchmark.IBenchmark;
import storm.benchmark.topology.common.WordCount;
import storm.benchmark.util.KafkaUtils;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;

import java.util.Map;

public class KafkaWordCount extends WordCount {

  private SpoutConfig kafkaConfig;

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    spout = new KafkaSpout(KafkaUtils.getSpoutConfig(options, new SchemeAsMultiScheme(new StringScheme())));

    return this;
  }
}
