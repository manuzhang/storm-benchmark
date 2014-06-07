package storm.benchmark.component.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import storm.benchmark.util.MockTupleHelpers;

import static org.mockito.Mockito.*;

public class ConstBoltTest {

  private ConstBolt bolt;
  private Tuple tuple;
  private BasicOutputCollector collector;
  private OutputFieldsDeclarer declarer;

  @BeforeMethod
  public void setUp() {
    bolt = new ConstBolt();
    tuple = MockTupleHelpers.mockAnyTuple();
    collector = mock(BasicOutputCollector.class);
    declarer = mock(OutputFieldsDeclarer.class);
  }

  @Test
  public void shouldDeclareOutputFields() {
    bolt.declareOutputFields(declarer);

    verify(declarer, times(1)).declare(any(Fields.class));
  }

  @Test
  public void shouldEmitFirstFieldOfTuple() {
    bolt.execute(tuple, collector);

    verify(tuple, times(1)).getValue(0);
    verify(collector, times(1)).emit(any(Values.class));
  }
}
