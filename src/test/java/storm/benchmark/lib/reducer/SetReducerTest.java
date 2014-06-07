package storm.benchmark.lib.reducer;

import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static org.fest.assertions.api.Assertions.assertThat;

public class SetReducerTest {

  private static final SetReducer<Object> reducer = new SetReducer<Object>();

  @Test
  public void zeroShouldReturnEmptySet() {
    assertThat(reducer.zero()).isEmpty();
  }

  @Test
  public void isZeroShouldReturnTrueOnEmptySet() {
    assertThat(reducer.isZero(new HashSet<Object>())).isTrue();
  }

  @Test (dataProvider = "getSets")
  public void reduceShouldReturnUnionOfTwoSets(Set set1, Set set2, Set union) {
    assertThat(reducer.reduce(set1, set2)).isEqualTo(union);
  }

  @DataProvider
  private Object[][] getSets() {
    return new Object[][] {
            { Sets.newHashSet(), Sets.newHashSet(1, 2, 3), Sets.newHashSet(1, 2, 3)},
            { Sets.newHashSet(1, 2, 3), Sets.newHashSet(), Sets.newHashSet(1, 2, 3)},
            { Sets.newHashSet(1, 2), Sets.newHashSet(3, 4), Sets.newHashSet(1, 2, 3, 4)},
            { Sets.newHashSet(1, 2, 3), Sets.newHashSet(2, 3, 4), Sets.newHashSet(1, 2, 3, 4)}
    };
  }

}
