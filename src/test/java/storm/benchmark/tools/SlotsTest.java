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

import org.apache.storm.utils.MutableObject;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import storm.benchmark.lib.reducer.Reducer;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static storm.benchmark.tools.SlidingWindow.Slots;

public class SlotsTest {
  private static final Object ANY_OBJECT = "ANY_OBJECT";
  private static final Object ANY_VALUE = "ANY_VALUE";
  private static final int ANY_NUM_SLOTS = 1;
  private static final int ANY_SLOT = 0;
  private static final Reducer ANY_REDUCER = mock(Reducer.class);


  @Test(expectedExceptions = IllegalArgumentException.class, dataProvider = "illegalNumSlots")
  public void lessThanOneSlotShouldThrowIAE(int numSlots) {
    new Slots<Object, Object>(ANY_REDUCER, numSlots);
  }

  @DataProvider
  public Object[][] illegalNumSlots() {
    return new Object[][] { {Integer.MIN_VALUE}, {-10}, {-1}, {0} };
  }

  @Test (dataProvider = "legalNumSlots")
  public void oneOrMoreSlotsShouldBeValid(int numSlots) {
    new Slots<Object, Object>(ANY_REDUCER, numSlots);
  }

  @DataProvider
  public Object[][] legalNumSlots() {
    return new Object[][] { {1}, {10}, {100}, {Integer.MAX_VALUE} };
  }

  @Test
  public void emptySlotsShouldNotContainAnyObject() {
    Slots slots = new Slots<Object, Object>(ANY_REDUCER, 2);
    assertThat(slots.contains(ANY_OBJECT)).isFalse();
  }

  @Test (expectedExceptions = IllegalArgumentException.class, dataProvider = "illegalSlot")
  public void addToIllegalSlotShouldThrowIAE(int numSlots, int slot) {
    Slots slots = new Slots<Object, Object>(ANY_REDUCER, numSlots);
    slots.add(ANY_OBJECT, ANY_VALUE, slot);
  }

  @DataProvider
  public Object[][] illegalSlot() {
    return new Object[][] {
            { 2, 2 }, { 2, 4 }, { 2, -1}
    };
  }


  @Test (dataProvider = "legalSlot")
  public void addToLegalSlotShouldSucceed(int numSlots, int slot) {
    Slots slots = new Slots<Object, Object>(ANY_REDUCER, numSlots);
    slots.add(ANY_OBJECT, ANY_VALUE, slot);
    assertThat(slots.contains(ANY_OBJECT)).isTrue();
    MutableObject m = slots.getValues(ANY_OBJECT)[slot];
    assertThat(m).isNotNull();
    assertThat(m.getObject()).isEqualTo(ANY_VALUE);
  }

  @DataProvider
  public Object[][] legalSlot() {
    return new Object[][] {
            { 3, 0 }, { 3, 2 }, { 300, 201 }
    };
  }


  @Test
  public void wipeZerosShouldRemoveObjWhoseReducedValueIsZero() {
    Slots slots = new Slots<Object, Object>(ANY_REDUCER, ANY_NUM_SLOTS);
    slots.add(ANY_OBJECT, ANY_VALUE, ANY_SLOT);
    assertThat(slots.contains(ANY_OBJECT)).isTrue();
    when(ANY_REDUCER.isZero(any(Object.class))).thenReturn(true);
    slots.wipeZeros();
    assertThat(slots.contains(ANY_OBJECT)).isFalse();
  }

  @Test (expectedExceptions = IllegalArgumentException.class, dataProvider = "illegalSlot")
  public void wipeIllegalSlotShouldThrowIAE(int numSlots, int slot) {
    Slots slots = new Slots<Object, Object>(ANY_REDUCER, numSlots);
    slots.wipeSlot(slot);
  }

  @Test (dataProvider = "legalSlot")
  public void wipeLegalSlotShouldWork(int numSlots, int slot) {
    Slots slots = new Slots<Object, Object>(ANY_REDUCER, numSlots);

    when(ANY_REDUCER.zero()).thenReturn(0);
    when(ANY_REDUCER.isZero(0)).thenReturn(true);

    slots.add(ANY_OBJECT, ANY_VALUE, slot);
    assertThat(slots.contains(ANY_OBJECT)).isTrue();
    assertThat(slots.getValues(ANY_OBJECT)[slot]).isNotNull();
    slots.wipeSlot(slot);
    MutableObject m = slots.getValues(ANY_OBJECT)[slot];
    assertThat(m).isNotNull();
    assertThat(ANY_REDUCER.isZero(m.getObject())).isTrue();
  }
 }
