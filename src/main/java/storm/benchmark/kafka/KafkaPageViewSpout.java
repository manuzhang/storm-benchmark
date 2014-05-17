package storm.benchmark.kafka;

import storm.benchmark.kafka.common.KafkaProducerSpout;
import storm.benchmark.tools.PageViewGenerator;

public class KafkaPageViewSpout extends KafkaProducerSpout {
  private static final long serialVersionUID = 1772211150101656352L;
  private PageViewGenerator generator;

  public KafkaPageViewSpout(int partitions) {
    super(partitions);
    this.generator = new PageViewGenerator();
  }

  @Override
  public void nextTuple() {
    nextMessage(generator.getNextClickEvent());
  }
}
