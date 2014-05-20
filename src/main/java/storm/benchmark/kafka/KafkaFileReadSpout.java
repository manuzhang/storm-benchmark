package storm.benchmark.kafka;

import storm.benchmark.kafka.common.KafkaProducerSpout;
import storm.benchmark.tools.FileReader;

public class KafkaFileReadSpout extends KafkaProducerSpout {

  private static final long serialVersionUID = -7503987913879480348L;
  private final FileReader reader;

  public KafkaFileReadSpout(String file) {
    this.reader = new FileReader(file);
  }

  @Override
  public void nextTuple() {
    nextMessage(reader.nextLine());
  }
}
