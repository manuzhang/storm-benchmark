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

package storm.benchmark.lib.reducer;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class LongSummerTest {

  private static final LongSummer summer = new LongSummer();
  private static final long NON_ZERO = 1L;

  @Test(dataProvider = "getLong")
  public void testReduce(long v1, long v2, long sum) {
    assertThat(summer.reduce(v1, v2)).isEqualTo(sum);
  }

  @DataProvider
  private Object[][] getLong() {
    return new Object[][] {
            { 1L, 1L, 2L },
            { 3L, 5L, 8L },
            { 13L, 21L, 34L }
    };
  }

  @Test
  public void testZero() {
    assertThat(summer.zero()).isEqualTo(0L);
  }

  @Test
  public void testIsZero() {
    assertThat(summer.isZero(0L)).isTrue();
    assertThat(summer.isZero(NON_ZERO)).isFalse();
  }

}
