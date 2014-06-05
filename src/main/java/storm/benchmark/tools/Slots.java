package storm.benchmark.tools;

import org.apache.log4j.Logger;
import storm.benchmark.reducer.Reducer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Slots<K, V> implements Serializable {

  private static final long serialVersionUID = 4858185737378394432L;
  private static final Logger LOG = Logger.getLogger(Slots.class);

  private final Map<K, Mutable[]> objToValues = new HashMap<K, Mutable[]>();
  private final int numSlots;
  private final Reducer<V> reducer;

  public Slots(Reducer reducer, int numSlots) {
    if (numSlots <= 0) {
      throw new IllegalArgumentException("Number of slots must be greater than zero (you requested " + numSlots + ")");
    }
    this.numSlots = numSlots;
    this.reducer = reducer;
  }

  public void add(K obj, V val, int slot) {
    if (slot < 0 || slot >= numSlots) {
      throw new IllegalArgumentException("the range of slot must be [0, numSlots)");
    }
    Mutable[] values = objToValues.get(obj);
    if (null == values) {
      values = initSlots(numSlots);
      objToValues.put(obj, values);
    }
    Mutable mut = values[slot];
    mut.set(reducer.reduce((V) mut.get(), val));
  }

  public Map<K, V> reduceByKey() {
    Map<K, V> reduced = new HashMap<K, V>();
    for (K obj : objToValues.keySet()) {
      reduced.put(obj, reduce(obj));
    }
    return reduced;
  }

  public V reduce(K obj) {
    if (!objToValues.containsKey(obj)) {
      LOG.warn("the object does not exist");
      return null;
    }
    Mutable[] values = objToValues.get(obj);
    final int len = values.length;
    V val = reducer.zero();
    for (int i = 0; i < len; i++) {
      Mutable mut = values[i];
      val = reducer.reduce(val, (V) mut.get());
    }
    return val;
  }

  public void wipeSlot(int slot) {
    if (slot < 0 || slot >= numSlots) {
      throw new IllegalArgumentException("the range of slot must be [0, numSlots)");
    }
    for (K obj : objToValues.keySet()) {
      Mutable m = objToValues.get(obj)[slot];
      if (m != null) {
        m.set(reducer.zero());
      }
    }
  }

  public void wipeZeros() {
    for (K obj : objToValues.keySet()) {
      if (shouldBeRemoved(obj)) {
        wipe(obj);
      }
    }
  }

  public boolean contains(K obj) {
    return objToValues.containsKey(obj);
  }

  public Mutable[] getValues(K obj) {
    return objToValues.get(obj);
  }

  private void wipe(K obj) {
    objToValues.remove(obj);
  }

  private boolean shouldBeRemoved(K obj) {
    return reducer.isZero(reduce(obj));
  }

  private Mutable[] initSlots(int numSlots) {
    Mutable[] muts = new Mutable[numSlots];
    for (int i = 0; i < numSlots; i++) {
      muts[i] = new Mutable();
    }
    return muts;
  }
}
