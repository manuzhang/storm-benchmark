package storm.benchmark.tools;

import java.io.Serializable;
import java.util.*;

public class Distribution<T> implements Serializable {

  private static final long serialVersionUID = 115923825637621162L;
  private final Map<T, Double> dist = new HashMap<T, Double>();

  public Distribution(List<Pair<T, Double>> data) {
    for (Pair<T, Double> p : data) {
      dist.put(p.key, p.val);
    }
  }

  public Distribution(Pair<T, Double>... data) {
    for (Pair<T, Double> p : data) {
      dist.put(p.key, p.val);
    }
  }

  public static Distribution intEvenDistribution(int start, int end) {
    if (start >= end) {
      throw new IllegalArgumentException(String.format("invalid arguments [%d, %d) to generate even distribution", start, end));
    }
    List<Pair<Integer, Double>> pairs = new ArrayList<Pair<Integer, Double>>();
    for (int i = start; i < end; i++) {
      pairs.add(new Pair(i, 1.0 / (end - start)));
    }
    return new Distribution(pairs);
  }

  public Set<T> getKeySet() {
    return dist.keySet();
  }

  public Double getProbability(T key) {
    return dist.get(key);
  }

  public static class Pair<K, V> {
    final K key;
    final V val;

    public Pair(K key, V val) {
      this.key = key;
      this.val = val;
    }
  }
}
