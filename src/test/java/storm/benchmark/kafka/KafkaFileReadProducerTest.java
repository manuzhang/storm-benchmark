package storm.benchmark.kafka;

import backtype.storm.Config;
import org.testng.annotations.Test;
import storm.benchmark.kafka.common.KafkaProducer;

import static org.fest.assertions.api.Assertions.assertThat;

public class KafkaFileReadProducerTest {

  @Test
  public void spoutShouldBeKafkaFileReadSpout() {
    KafkaProducer producer = new KafkaFileReadProducer();
    producer.getTopology(new Config());
    assertThat(producer.getSpout()).isInstanceOf(KafkaFileReadSpout.class);
  }


}
