package storm.benchmark.util;

import backtype.storm.generated.ComponentCommon;

import static org.fest.assertions.api.Assertions.assertThat;

public class TestUtils {
  private TestUtils() {
  }

  public static void verifyParallelism(ComponentCommon component, int expected) {
    assertThat(component.get_parallelism_hint()).isEqualTo(expected);
  }
}
