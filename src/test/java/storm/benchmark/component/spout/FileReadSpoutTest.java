package storm.benchmark.component.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import storm.benchmark.util.FileReader;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

public class FileReadSpoutTest {
  private static final int messageSize = 100;
  private static final Map ANY_CONF = new HashMap();
  private static final String NEXT_LINE = "next line";
  private OutputFieldsDeclarer declarer;
  private TopologyContext context;
  private SpoutOutputCollector collector;
  private FileReader reader;

  @BeforeMethod
  public void setUp() {
    declarer = mock(OutputFieldsDeclarer.class);
    collector = mock(SpoutOutputCollector.class);
    context = mock(TopologyContext.class);
    reader = mock(FileReader.class);
    when(reader.nextLine()).thenReturn(NEXT_LINE);
  }

  @Test
  public void shouldDeclareOutputFields() {
    FileReadSpout spout = new FileReadSpout(false, reader);

    spout.declareOutputFields(declarer);

    verify(declarer, times(1)).declare(any(Fields.class));
  }

  @Test
  public void shouldEmitValueAndIdWhenAckEnabled() {
    FileReadSpout spout = new FileReadSpout(true, reader);

    spout.open(ANY_CONF, context, collector);
    spout.nextTuple();

    verify(collector, times(1)).emit(any(Values.class), anyInt());
  }

  @Test
  public void shouldEmitValueOnlyWhenAckDisabled() {
    FileReadSpout spout = new FileReadSpout(false, reader);

    spout.open(ANY_CONF, context, collector);
    spout.nextTuple();

    verify(collector, times(1)).emit(any(Values.class));
  }
}
