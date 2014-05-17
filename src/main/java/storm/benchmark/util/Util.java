package storm.benchmark.util;

import java.util.List;
import java.util.Map;

public final class Util {

  private Util() {
  }

  public static String join(List<String> list, String sep) {
    StringBuilder sb = new StringBuilder();
    for (String s : list) {
      sb.append(s);
      sb.append(sep);
    }
    sb.deleteCharAt(sb.lastIndexOf(sep));
    return sb.toString();
  }

  public static double max(List<Double> list) {
    double max = Double.MIN_VALUE;
    for (double d : list) {
      if (d > max) {
        max = d;
      }
    }
    return max;
  }

  public static double avg(List<Double> list) {
    double total = 0.0;
    int num = 0;
    for (double d : list) {
      total += d;
      num++;
    }
    return total / num;
  }

  public static int retIfPositive(int defVal, Integer newVal) {
    if (null == newVal || newVal <= 0) {
      return defVal;
    } else {
      return newVal;
    }
  }

  public static Object retIfNotNull(Object defVal, Object newVal) {
    return (null == newVal) ? defVal : newVal;
  }

  public static void putIfAbsent(Map map, Object key, Object val) {
    if (!map.containsKey(key)) {
      map.put(key, val);
    }
  }
}
