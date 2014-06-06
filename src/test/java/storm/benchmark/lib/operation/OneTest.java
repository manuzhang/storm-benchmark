package storm.benchmark.lib.operation;

import org.testng.annotations.Test;
import storm.trident.tuple.TridentTuple;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class OneTest {
  private static final int ANY_INT_ONE = 1;
  private static final int ANY_INT_TWO = 1;
  private static final One one = new One();

  @Test
  public void testInit() throws Exception {
    TridentTuple tuple = mock(TridentTuple.class);
    assertThat(one.init(tuple)).isEqualTo(1);
  }

  @Test
  public void testCombine() throws Exception {
    assertThat(one.combine(ANY_INT_ONE, ANY_INT_TWO)).isEqualTo(1);
  }

  @Test
  public void testZero() throws Exception {
    assertThat(one.zero()).isEqualTo(0);
  }
}
