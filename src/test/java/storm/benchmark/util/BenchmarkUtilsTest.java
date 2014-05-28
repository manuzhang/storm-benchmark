package storm.benchmark.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;

public class BenchmarkUtilsTest {

  @Test(dataProvider = "getAvgs")
  public void testAvg(Iterable<Double> numbers, double expected) {
    assertThat(BenchmarkUtils.avg(numbers)).isEqualTo(expected);
  }

  @DataProvider
  private Object[][] getAvgs() {
    return new Object[][] {
            { Lists.newArrayList(1.0, 3.0, 8.0), 4.0 },
            { Sets.newHashSet(2.5, 4.5, 9.5), 5.5 }
    };
  }

  @Test(dataProvider = "getMaxes")
  public void testMax(Iterable<Double> numbers, double expected) {
    assertThat(BenchmarkUtils.max(numbers)).isEqualTo(expected);
  }

  @DataProvider
  private Object[][] getMaxes() {
    return new Object[][] {
            { Lists.newArrayList(1.0, 3.0, 3.0), 3.0 },
            { Sets.newHashSet(2.5, 4.5, 6.5), 6.5 }
    };
  }

  @Test
  public void testPutIfAbsent() {
    Map<String, Integer> map = Maps.newHashMap();
    BenchmarkUtils.putIfAbsent(map, "key1", 1);
    assertThat(map.get("key1")).isEqualTo(1);
    BenchmarkUtils.putIfAbsent(map, "key1", 2);
    assertThat(map.get("key1")).isEqualTo(1);
    BenchmarkUtils.putIfAbsent(map, "key2", 3);
    assertThat(map.get("key2")).isEqualTo(3);
  }

  @Test
  public void testGetInt() {
    Map map = Maps.newHashMap();
    assertThat(BenchmarkUtils.getInt(map, "key1", 1)).isEqualTo(1);
    map.put("key1", 2);
    assertThat(BenchmarkUtils.getInt(map, "key1", 1)).isEqualTo(2);
    map.put("key2", 3L);
    assertThat(BenchmarkUtils.getInt(map, "key2", 1)).isEqualTo(3);
  }

}
