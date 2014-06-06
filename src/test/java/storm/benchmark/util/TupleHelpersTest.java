package storm.benchmark.util;

import org.testng.annotations.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class TupleHelpersTest {

  @Test
  public void testIsTickTuple() {
    assertThat(TupleHelpers.isTickTuple(MockTupleHelpers.mockTickTuple())).isTrue();
    assertThat(TupleHelpers.isTickTuple(MockTupleHelpers.mockAnyTuple())).isFalse();
  }
}
