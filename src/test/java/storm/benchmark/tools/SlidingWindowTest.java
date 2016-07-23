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

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import storm.benchmark.lib.reducer.LongSummer;
import storm.benchmark.lib.reducer.Reducer;

import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;


/** forked from SlidingWindowCounterTest in storm-starter */

public class SlidingWindowTest {
  private static final Object ANY_OBJECT = "ANY_OBJECT";

  @DataProvider
  public Object[][] illegalWindowLengths() {
    return new Object[][]{ { -10 }, { -3 }, { -2 }, { -1 }, { 0 }, { 1 } };
  }

  @Test(expectedExceptions = IllegalArgumentException.class, dataProvider = "illegalWindowLengths")
  public void lessThanTwoSlotsShouldThrowIAE(int windowLengthInSlots) {
    Reducer reducer = mock(Reducer.class);
    new SlidingWindow<Object, Object>(reducer, windowLengthInSlots);
  }

  @DataProvider
  public Object[][] legalWindowLengths() {
    return new Object[][]{ { 2 }, { 3 }, { 20 } };
  }

  @Test(dataProvider = "legalWindowLengths")
  public void twoOrMoreSlotsShouldBeValid(int windowLengthInSlots) {
    Reducer reducer = mock(Reducer.class);
    new SlidingWindow<Object, Object>(reducer, windowLengthInSlots);
  }

  @DataProvider
  public Object[][] simulatedCounterIterations() {
    return new Object[][]{
            { 2, new int[]{ 3, 2, 0, 0, 1, 0, 0, 0 }, new long[]{ 3, 5, 2, 0, 1, 1, 0, 0 } },
            { 3, new int[]{ 3, 2, 0, 0, 1, 0, 0, 0 }, new long[]{ 3, 5, 5, 2, 1, 1, 1, 0 } },
            { 4, new int[]{ 3, 2, 0, 0, 1, 0, 0, 0 }, new long[]{ 3, 5, 5, 5, 3, 1, 1, 1 } },
            { 5, new int[]{ 3, 2, 0, 0, 1, 0, 0, 0 }, new long[]{ 3, 5, 5, 5, 6, 3, 1, 1 } },
            { 5, new int[]{ 3, 11, 5, 13, 7, 17, 0, 3, 50, 600, 7000 },
                    new long[]{ 3, 14, 19, 32, 39, 53, 42, 40, 77, 670, 7653 } }
    };
  }




  @Test(dataProvider = "simulatedCounterIterations")
  public void testCounterWithSimulatedRuns(int windowLengthInSlots, int[] incrementsPerIteration,
                                           long[] expCountsPerIteration) {
    // given
    SlidingWindow<Object, Long> counter = new SlidingWindow<Object, Long>(new LongSummer(), windowLengthInSlots);
    int numIterations = incrementsPerIteration.length;

    for (int i = 0; i < numIterations; i++) {
      int numIncrements = incrementsPerIteration[i];
      long expCounts = expCountsPerIteration[i];

      // given (for this iteration)
      if (0 == numIncrements) {
        counter.add(ANY_OBJECT, (long) 0);
      }
      for (int j = 0; j < numIncrements; j++) {
        counter.add(ANY_OBJECT, (long) 1);
      }

      // when (for this iteration)
      Map<Object, Long> counts = counter.reduceThenAdvanceWindow();

      // then (for this iteration)
      assertThat(counts.get(ANY_OBJECT)).isEqualTo(expCounts);
    }
  }

}
