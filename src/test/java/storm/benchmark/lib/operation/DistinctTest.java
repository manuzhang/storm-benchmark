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

package storm.benchmark.lib.operation;

import com.google.common.collect.Sets;
import org.apache.storm.trident.tuple.TridentTuple;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DistinctTest {
  private static final Set<Integer> ANY_INT_SET_ONE = Sets.newHashSet(1, 2, 3);
  private static final Set<Integer> ANY_INT_SET_TWO = Sets.newHashSet(2, 3, 4);

  private static final Distinct distinct = new Distinct();


  @Test
  public void testInit() throws Exception {
    TridentTuple tuple = mock(TridentTuple.class);
    when(tuple.getInteger(1)).thenReturn(1);
    Set<Integer> init = distinct.init(tuple);
    assertThat(init)
            .isNotNull()
            .hasSize(1)
            .contains(1);
  }

  @Test
  public void testCombine() throws Exception {
    Set<Integer> combined = distinct.combine(ANY_INT_SET_ONE, ANY_INT_SET_TWO);
    assertThat(combined)
            .isNotNull()
            .hasSize(4)
            .contains(1, 2, 3, 4);
  }

  @Test
  public void testZero() throws Exception {
    assertThat(distinct.zero())
            .isNotNull()
            .isInstanceOf(HashSet.class)
            .isEmpty();
  }
}
