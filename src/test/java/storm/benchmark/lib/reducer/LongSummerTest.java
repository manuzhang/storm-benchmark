package storm.benchmark.lib.reducer;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class LongSummerTest {

  private static final LongSummer summer = new LongSummer();
  private static final long NON_ZERO = 1L;

  @Test(dataProvider = "getLong")
  public void testReduce() {
    assertThat(summer.reduce(1L, 2L)).isEqualTo(3L);
  }

  @DataProvider
  private Object[][] getLong() {
    return new Object[][] {
            { 1L, 1L, 2L },
            { 3L, 5L, 8L },
            { 13L, 21L, 34L }
    };
  }

  @Test
  public void testZero() {
    assertThat(summer.zero()).isEqualTo(0L);
  }

  @Test
  public void testIsZero() {
    assertThat(summer.isZero(0L)).isTrue();
    assertThat(summer.isZero(NON_ZERO)).isFalse();
  }

}
