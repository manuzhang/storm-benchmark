package storm.benchmark.kafka;

import storm.benchmark.IBenchmark;
import storm.benchmark.kafka.common.KafkaProducer;

import java.util.Map;

public class KafkaFileReadProducer extends KafkaProducer {

  public static final String FILE = "/resources/A_Tale_of_Two_City.txt";

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    spout = new KafkaFileReadSpout(FILE);
    return this;
  }
}
