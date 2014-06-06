package storm.benchmark.util;


import storm.benchmark.lib.reducer.Reducer;

import java.io.Serializable;
import java.util.Map;

public class SlidingWindow<K, V> implements Serializable {
  private static final long serialVersionUID = -2645063988768785810L;

  private final Slots<K, V> slots;
  private int headSlot;
  private int tailSlot;
  private final int windowLengthInSlots;
  private final Reducer reducer;

  public SlidingWindow(Reducer reducer, int windowLengthInSlots) {
    if (windowLengthInSlots < 2) {
      throw new IllegalArgumentException(
              "Window length in slots must be at least two (you requested " + windowLengthInSlots + ")");
    }
    this.windowLengthInSlots = windowLengthInSlots;
    this.reducer = reducer;
    this.slots = new Slots(this.reducer, windowLengthInSlots);

    this.headSlot = 0;
    this.tailSlot = slotAfter(headSlot);
  }

  public void add(K obj, V val) {
    slots.add(obj, val, headSlot);
  }

  public Map<K, V> reduceThenAdvanceWindow() {
    Map<K, V> reduced = slots.reduceByKey();
    slots.wipeSlot(tailSlot);
    slots.wipeZeros();
    advanceHead();
    return reduced;
  }

  private void advanceHead() {
    headSlot = tailSlot;
    tailSlot = slotAfter(tailSlot);
  }

  private int slotAfter(int slot) {
    return (slot + 1) % windowLengthInSlots;
  }

}
