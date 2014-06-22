/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package storm.benchmark.tools;


import backtype.storm.utils.MutableObject;
import org.apache.log4j.Logger;
import storm.benchmark.lib.reducer.Reducer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

  public static class Slots<K, V> implements Serializable {

    private static final long serialVersionUID = 4858185737378394432L;
    private static final Logger LOG = Logger.getLogger(Slots.class);

    private final Map<K, MutableObject[]> objToValues = new HashMap<K, MutableObject[]>();
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
      MutableObject[] values = objToValues.get(obj);
      if (null == values) {
        values = initSlots(numSlots, slot, val);
        objToValues.put(obj, values);
      } else {
        MutableObject mut = values[slot];
        mut.setObject(reducer.reduce((V) mut.getObject(), val));
      }
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
      MutableObject[] values = objToValues.get(obj);
      final int len = values.length;
      V val = reducer.zero();
      for (int i = 0; i < len; i++) {
        MutableObject mut = values[i];
        val = reducer.reduce(val, (V) mut.getObject());
      }
      return val;
    }

    public void wipeSlot(int slot) {
      if (slot < 0 || slot >= numSlots) {
        throw new IllegalArgumentException("the range of slot must be [0, numSlots)");
      }
      for (K obj : objToValues.keySet()) {
        MutableObject m = objToValues.get(obj)[slot];
        if (m != null) {
          m.setObject(reducer.zero());
        }
      }
    }

    public void wipeZeros() {
      Set<K> toBeRemoved = new HashSet<K>();
      for (K obj : objToValues.keySet()) {
        if (shouldBeRemoved(obj)) {
          toBeRemoved.add(obj);
        }
      }
      for (K obj : toBeRemoved) {
        wipe(obj);
      }
    }

    public boolean contains(K obj) {
      return objToValues.containsKey(obj);
    }

    public MutableObject[] getValues(K obj) {
      return objToValues.get(obj);
    }

    private void wipe(K obj) {
      objToValues.remove(obj);
    }

    private boolean shouldBeRemoved(K obj) {
      return reducer.isZero(reduce(obj));
    }

    private MutableObject[] initSlots(int numSlots, int slot, V val) {
      MutableObject[] muts = new MutableObject[numSlots];
      for (int i = 0; i < numSlots; i++) {
        if (i == slot) {
          muts[i] = new MutableObject(val);
        } else {
          muts[i] = new MutableObject(reducer.zero());
        }
      }
      return muts;
    }
  }
}
