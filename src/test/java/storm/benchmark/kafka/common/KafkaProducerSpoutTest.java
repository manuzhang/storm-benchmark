package storm.benchmark.kafka.common;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import storm.benchmark.kafka.KafkaFileReadSpout;
import storm.benchmark.kafka.KafkaPageViewSpout;
import storm.benchmark.util.FileReader;

import static org.mockito.Mockito.*;

public class KafkaProducerSpoutTest {

  @Test(dataProvider = "getKafkaProducerSpout")
  public void shouldDeclareOutputFields(KafkaProducerSpout spout) {
    OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);

    spout.declareOutputFields(declarer);

    verify(declarer, times(1)).declare(any(Fields.class));
  }

  @DataProvider
  private Object[][] getKafkaProducerSpout() {
    return new Object[][] {
            { new KafkaFileReadSpout(mock(FileReader.class)) },
            { new KafkaPageViewSpout() }
    } ;
  }
}
