package storm.benchmark.kafka;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import org.testng.annotations.Test;
import storm.benchmark.component.spout.pageview.PageViewGenerator;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class KafkaPageViewSpoutTest {
  private static final Map ANY_CONF = new HashMap();

  @Test
  public void nextTupleShouldEmitNextClickEvent() throws Exception {
    PageViewGenerator generator = mock(PageViewGenerator.class);
    String pageView = "page_view";
    KafkaPageViewSpout spout = new KafkaPageViewSpout(generator);
    TopologyContext context = mock(TopologyContext.class);
    SpoutOutputCollector collector = mock(SpoutOutputCollector.class);

    when(generator.getNextClickEvent()).thenReturn(pageView);

    spout.open(ANY_CONF, context, collector);
    spout.nextTuple();

    verify(collector, times(1)).emit(any(Values.class));
  }
}
