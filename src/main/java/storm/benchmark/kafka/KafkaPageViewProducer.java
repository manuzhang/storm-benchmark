package storm.benchmark.kafka;

import storm.benchmark.IBenchmark;
import storm.benchmark.kafka.common.KafkaProducer;

import java.util.Map;

public class KafkaPageViewProducer extends KafkaProducer {

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    spout = new KafkaPageViewSpout();

    return this;
  }
}
