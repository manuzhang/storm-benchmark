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

import org.testng.annotations.Test;
import storm.trident.tuple.TridentTuple;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class OneTest {
  private static final int ANY_INT_ONE = 1;
  private static final int ANY_INT_TWO = 1;
  private static final One one = new One();

  @Test
  public void testInit() throws Exception {
    TridentTuple tuple = mock(TridentTuple.class);
    assertThat(one.init(tuple)).isEqualTo(1);
  }

  @Test
  public void testCombine() throws Exception {
    assertThat(one.combine(ANY_INT_ONE, ANY_INT_TWO)).isEqualTo(1);
  }

  @Test
  public void testZero() throws Exception {
    assertThat(one.zero()).isEqualTo(1);
  }
}
