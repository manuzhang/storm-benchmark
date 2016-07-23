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

package storm.benchmark.lib.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import storm.benchmark.util.MockTupleHelpers;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class FilterBoltTest {
  private static final String TO_FILTER = "to_filter";
  private static final String NON_FILTER = "non_filter";
  private FilterBolt bolt;
  private Tuple tuple;
  private BasicOutputCollector collector;
  private OutputFieldsDeclarer declarer;

  @BeforeMethod
  public void setUp() {
    bolt = new FilterBolt(TO_FILTER);
    tuple = MockTupleHelpers.mockAnyTuple();
    collector = mock(BasicOutputCollector.class);
    declarer = mock(OutputFieldsDeclarer.class);

  }

  @Test
  public void shouldDeclareOutputFields() {
    bolt.declareOutputFields(declarer);

    verify(declarer, times(1)).declare(any(Fields.class));
  }

  @Test
  public void shouldNotEmitIfFiltered() {
    when(tuple.getValue(0)).thenReturn(TO_FILTER);

    bolt.execute(tuple, collector);

    verify(tuple, never()).getValue(1);
    verifyZeroInteractions(collector);
  }

  @Test
  public void shouldEmitSecondFieldIfNotFiltered() {
    when(tuple.getValue(0)).thenReturn(NON_FILTER);

    bolt.execute(tuple, collector);

    verify(tuple, times(1)).getValue(1);
    verify(collector, times(1)).emit(any(Values.class));
  }
}
