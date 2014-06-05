package storm.benchmark.kafka;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import storm.benchmark.kafka.common.KafkaProducer;

public class KafkaFileReadProducer extends KafkaProducer {

  public static final String FILE = "/resources/A_Tale_of_Two_City.txt";

  @Override
  public StormTopology getTopology(Config config) {
    spout = new KafkaFileReadSpout(FILE);
    return super.getTopology(config);
  }
}
