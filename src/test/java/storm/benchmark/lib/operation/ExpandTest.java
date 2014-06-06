package storm.benchmark.lib.operation;

import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class ExpandTest {


  @Test (dataProvider = "getIterable")
  public void testExpand(Iterable iterable, int size)  {
    final TridentTuple tuple = mock(TridentTuple.class);
    final TridentCollector collector = mock(TridentCollector.class);
    when(tuple.getValue(0)).thenReturn(iterable);
    Expand expand = new Expand();
    expand.execute(tuple, collector);

    verify(tuple, times(1)).getValue(0);
    verify(collector, times(size)).emit(any(Values.class));
  }

  @DataProvider
  private Object[][] getIterable() {
    return new Object[][] {
            { Lists.newArrayList(1, 2, 3), 3},
            { Sets.newHashSet(1, 2, 3), 3}
    };
  }
}
