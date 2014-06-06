package storm.benchmark.component.bolt.common;

import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import storm.benchmark.component.bolt.RollingCountBolt;
import storm.benchmark.component.bolt.UniqueVisitorBolt;
import storm.benchmark.util.MockTupleHelpers;

import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class RollingBoltTest {
  private static final String ANY_NON_SYSTEM_COMPONENT_ID = "irrelevant_component_id";
  private static final String ANY_NON_SYSTEM_STREAM_ID = "irrelevant_stream_id";

  private Tuple mockNormalTuple(Object obj) {
    Tuple tuple = MockTupleHelpers.mockTuple(ANY_NON_SYSTEM_COMPONENT_ID, ANY_NON_SYSTEM_STREAM_ID);
    when(tuple.getValue(0)).thenReturn(obj);
    return tuple;
  }

  @Test(dataProvider = "getRollingBolt")
  public void shouldEmitNothingIfNoObjectHasBeenCountedYetAndTickTupleIsReceived(RollingBolt bolt) {
    Tuple tickTuple = MockTupleHelpers.mockTickTuple();
    BasicOutputCollector collector = mock(BasicOutputCollector.class);

    bolt.execute(tickTuple, collector);

    verifyZeroInteractions(collector);
  }

  @Test(dataProvider = "getRollingBolt")
  public void shouldEmitSomethingIfAtLeastOneObjectWasCountedAndTickTupleIsReceived(RollingBolt bolt) {
    Tuple normalTuple = mockNormalTuple(new Object());
    Tuple tickTuple = MockTupleHelpers.mockTickTuple();
    BasicOutputCollector collector = mock(BasicOutputCollector.class);

    bolt.execute(normalTuple, collector);
    bolt.execute(tickTuple, collector);

    verify(collector).emit(any(Values.class));
  }

  @Test(dataProvider = "getRollingBolt")
  public void shouldDeclareOutputFields(RollingBolt bolt) {
    OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);

    bolt.declareOutputFields(declarer);

    verify(declarer, times(1)).declare(any(Fields.class));
  }


  @Test(dataProvider = "getRollingBolt")
  public void shouldSetTickTupleFrequencyInComponentConfigurationToNonZeroValue(RollingBolt bolt) {
    Map<String, Object> componentConfig = bolt.getComponentConfiguration();

    assertThat(componentConfig).containsKey(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
    Integer emitFrequencyInSeconds = (Integer) componentConfig.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
    assertThat(emitFrequencyInSeconds).isGreaterThan(0);
  }

  @DataProvider
  private Object[][] getRollingBolt() {
    return new Object[][] {
            { new RollingCountBolt() },
            { new UniqueVisitorBolt() }
    };
  }
}
