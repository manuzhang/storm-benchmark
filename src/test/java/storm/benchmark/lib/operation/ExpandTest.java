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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExpandTest {


  @Test (dataProvider = "getIterable")
  public void testExpand(Iterable iterable, int size)  {
    final TridentTuple tuple = mock(TridentTuple.class);
    final TridentCollector collector = mock(TridentCollector.class);
    when(tuple.getValue(0)).thenReturn(iterable);
    Expand expand = new Expand();
    expand.execute(tuple, collector);

    verify(tuple, times(1)).getValue(0);
    verify(collector, times(size)).emit(any(Values.class));
  }

  @DataProvider
  private Object[][] getIterable() {
    return new Object[][] {
            { Lists.newArrayList(1, 2, 3), 3},
            { Sets.newHashSet(1, 2, 3), 3}
    };
  }
}
