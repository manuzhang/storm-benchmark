package storm.benchmark.kafka;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import storm.benchmark.kafka.common.KafkaProducer;

public class KafkaPageViewProducer extends KafkaProducer {

  @Override
  public StormTopology getTopology(Config config) {
    spout = new KafkaPageViewSpout();
    return super.getTopology(config);
  }
}
