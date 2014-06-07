package storm.benchmark.component.spout;

import backtype.storm.tuple.Values;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import storm.benchmark.util.FileReader;
import storm.trident.operation.TridentCollector;

import java.util.List;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class TridentFileReadSpoutTest {

  private static final String NEXT_LINE = "next line";
  private FileReader reader;
  private TridentFileReadSpout spout;
  private TridentCollector collector;

  @BeforeMethod
  public void setUp() {
    reader = mock(FileReader.class);
    spout = new TridentFileReadSpout(10, reader);
    collector = mock(TridentCollector.class);
    when(reader.nextLine()).thenReturn(NEXT_LINE);
  }

  @Test
  public void testEmitBatch() {
    spout.emitBatch(0L, collector);
    spout.emitBatch(1L, collector);

    verify(collector, times(20)).emit(any(Values.class));

    Map<Long, List<String>> batches = spout.getBatches();
    assertThat(batches)
            .hasSize(2)
            .containsKey(0L)
            .containsKey(1L);
    assertThat(batches.get(0L)).hasSize(10);
    assertThat(batches.get(1L)).hasSize(10);
  }

  @Test
  public void ackShouldRemoveBatch() {
    spout.emitBatch(0L, collector);
    Map<Long, List<String>> batches = spout.getBatches();
    assertThat(batches).containsKey(0L);

    spout.ack(0L);
    assertThat(batches).isEmpty();
  }

}
