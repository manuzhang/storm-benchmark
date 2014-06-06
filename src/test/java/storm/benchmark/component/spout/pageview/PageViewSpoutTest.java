package storm.benchmark.component.spout.pageview;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import storm.benchmark.util.PageViewGenerator;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

public class PageViewSpoutTest {
  private static final Map ANY_CONF = new HashMap();
  private static final String NEXT_CLICK_EVENT = "next click event";
  private OutputFieldsDeclarer declarer;
  private TopologyContext context;
  private SpoutOutputCollector collector;
  private PageViewGenerator generator;

  @BeforeMethod
  public void setUp() {
    declarer = mock(OutputFieldsDeclarer.class);
    collector = mock(SpoutOutputCollector.class);
    context = mock(TopologyContext.class);
    generator = mock(PageViewGenerator.class);
    when(generator.getNextClickEvent()).thenReturn(NEXT_CLICK_EVENT);
  }

  @Test
  public void shouldDeclareOutputFields() {
    PageViewSpout spout = new PageViewSpout(false, generator);

    spout.declareOutputFields(declarer);

    verify(declarer, times(1)).declare(any(Fields.class));
  }

  @Test
  public void shouldEmitValueAndIdWhenAckEnabled() {
    PageViewSpout spout = new PageViewSpout(true, generator);

    spout.open(ANY_CONF, context, collector);
    spout.nextTuple();

    verify(collector, times(1)).emit(any(Values.class), anyInt());
  }

  @Test
  public void shouldEmitValueOnlyWhenAckDisabled() {
    PageViewSpout spout = new PageViewSpout(false, generator);

    spout.open(ANY_CONF, context, collector);
    spout.nextTuple();

    verify(collector, times(1)).emit(any(Values.class));
  }
}
