package storm.benchmark.util;

import backtype.storm.utils.Utils;

import java.util.Iterator;
import java.util.Map;

public final class BenchmarkUtils {

  private BenchmarkUtils() {
  }

  public static double max(Iterable<Double> iterable) {
    Iterator<Double> iterator = iterable.iterator();
    double max = Double.MIN_VALUE;
    while (iterator.hasNext()) {
      double d = iterator.next();
      if (d > max) {
        max = d;
      }
    }
    return max;
  }

  public static double avg(Iterable<Double> iterable) {
    Iterator<Double> iterator = iterable.iterator();
    double total = 0.0;
    int num = 0;
    while (iterator.hasNext()) {
      total += iterator.next();
      num++;
    }
    return total / num;
  }

  public static void putIfAbsent(Map map, Object key, Object val) {
    if (!map.containsKey(key)) {
      map.put(key, val);
    }
  }

  public static int getInt(Map map, Object key, int def) {
    return Utils.getInt(Utils.get(map, key, def));
  }
}
