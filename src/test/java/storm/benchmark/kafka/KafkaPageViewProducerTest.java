package storm.benchmark.kafka;

import backtype.storm.Config;
import org.testng.annotations.Test;
import storm.benchmark.kafka.common.KafkaProducer;

import static org.fest.assertions.api.Assertions.assertThat;

public class KafkaPageViewProducerTest {
  @Test
  public void spoutShouldBeKafkaPageViewSpout() {
    KafkaProducer producer = new KafkaPageViewProducer();
    producer.getTopology(new Config());
    assertThat(producer.getSpout()).isInstanceOf(KafkaPageViewSpout.class);
  }
}
