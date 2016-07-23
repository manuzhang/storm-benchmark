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

package storm.benchmark.lib.bolt.common;

import org.apache.storm.Config;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import storm.benchmark.lib.bolt.RollingBolt;
import storm.benchmark.lib.bolt.RollingCountBolt;
import storm.benchmark.lib.bolt.UniqueVisitorBolt;
import storm.benchmark.util.MockTupleHelpers;

import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class RollingBoltTest {
  private static final String ANY_NON_SYSTEM_COMPONENT_ID = "irrelevant_component_id";
  private static final String ANY_NON_SYSTEM_STREAM_ID = "irrelevant_stream_id";

  private Tuple mockNormalTuple(Object obj) {
    Tuple tuple = MockTupleHelpers.mockTuple(ANY_NON_SYSTEM_COMPONENT_ID, ANY_NON_SYSTEM_STREAM_ID);
    when(tuple.getValue(0)).thenReturn(obj);
    return tuple;
  }

  @Test(dataProvider = "getRollingBolt")
  public void shouldEmitNothingIfNoObjectHasBeenCountedYetAndTickTupleIsReceived(RollingBolt bolt) {
    Tuple tickTuple = MockTupleHelpers.mockTickTuple();
    BasicOutputCollector collector = mock(BasicOutputCollector.class);

    bolt.execute(tickTuple, collector);

    verifyZeroInteractions(collector);
  }

  @Test(dataProvider = "getRollingBolt")
  public void shouldEmitSomethingIfAtLeastOneObjectWasCountedAndTickTupleIsReceived(RollingBolt bolt) {
    Tuple normalTuple = mockNormalTuple(new Object());
    Tuple tickTuple = MockTupleHelpers.mockTickTuple();
    BasicOutputCollector collector = mock(BasicOutputCollector.class);

    bolt.execute(normalTuple, collector);
    bolt.execute(tickTuple, collector);

    verify(collector).emit(any(Values.class));
  }

  @Test(dataProvider = "getRollingBolt")
  public void shouldDeclareOutputFields(RollingBolt bolt) {
    OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);

    bolt.declareOutputFields(declarer);

    verify(declarer, times(1)).declare(any(Fields.class));
  }


  @Test(dataProvider = "getRollingBolt")
  public void shouldSetTickTupleFrequencyInComponentConfigurationToNonZeroValue(RollingBolt bolt) {
    Map<String, Object> componentConfig = bolt.getComponentConfiguration();

    assertThat(componentConfig).containsKey(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
    Integer emitFrequencyInSeconds = (Integer) componentConfig.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
    assertThat(emitFrequencyInSeconds).isGreaterThan(0);
  }

  @DataProvider
  private Object[][] getRollingBolt() {
    return new Object[][] {
            { new RollingCountBolt() },
            { new UniqueVisitorBolt() }
    };
  }
}
