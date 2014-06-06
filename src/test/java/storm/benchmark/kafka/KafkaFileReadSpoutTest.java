package storm.benchmark.kafka;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import org.testng.annotations.Test;
import storm.benchmark.util.FileReader;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

public class KafkaFileReadSpoutTest {
  private static final Map ANY_CONF = new HashMap();

  @Test
  public void nextTupleShouldEmitNextLineOfFile() throws Exception {
    FileReader reader = mock(FileReader.class);
    String message = "line";
    KafkaFileReadSpout spout = new KafkaFileReadSpout(reader);
    TopologyContext context = mock(TopologyContext.class);
    SpoutOutputCollector collector = mock(SpoutOutputCollector.class);

    when(reader.nextLine()).thenReturn(message);

    spout.open(ANY_CONF, context, collector);
    spout.nextTuple();

    verify(collector, times(1)).emit(any(Values.class));
  }
}
