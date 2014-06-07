package storm.benchmark.kafka;

import storm.benchmark.kafka.common.KafkaProducerSpout;
import storm.benchmark.util.PageViewGenerator;

public class KafkaPageViewSpout extends KafkaProducerSpout {
  private static final long serialVersionUID = 1772211150101656352L;
  private PageViewGenerator generator;

  public KafkaPageViewSpout() {
    this.generator = new PageViewGenerator();
  }

  public KafkaPageViewSpout(PageViewGenerator generator) {
    this.generator = generator;
  }

  @Override
  public void nextTuple() {
    nextMessage(generator.getNextClickEvent());
  }
}
