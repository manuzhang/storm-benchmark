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

import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static org.fest.assertions.api.Assertions.assertThat;

public class SetReducerTest {

  private static final SetReducer<Object> reducer = new SetReducer<Object>();

  @Test
  public void zeroShouldReturnEmptySet() {
    assertThat(reducer.zero()).isEmpty();
  }

  @Test
  public void isZeroShouldReturnTrueOnEmptySet() {
    assertThat(reducer.isZero(new HashSet<Object>())).isTrue();
  }

  @Test (dataProvider = "getSets")
  public void reduceShouldReturnUnionOfTwoSets(Set set1, Set set2, Set union) {
    assertThat(reducer.reduce(set1, set2)).isEqualTo(union);
  }

  @DataProvider
  private Object[][] getSets() {
    return new Object[][] {
            { Sets.newHashSet(), Sets.newHashSet(1, 2, 3), Sets.newHashSet(1, 2, 3)},
            { Sets.newHashSet(1, 2, 3), Sets.newHashSet(), Sets.newHashSet(1, 2, 3)},
            { Sets.newHashSet(1, 2), Sets.newHashSet(3, 4), Sets.newHashSet(1, 2, 3, 4)},
            { Sets.newHashSet(1, 2, 3), Sets.newHashSet(2, 3, 4), Sets.newHashSet(1, 2, 3, 4)}
    };
  }

}
