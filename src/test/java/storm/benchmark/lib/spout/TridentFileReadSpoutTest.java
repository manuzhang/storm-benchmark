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

package storm.benchmark.lib.spout;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.tuple.Values;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import storm.benchmark.tools.FileReader;

import java.util.List;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TridentFileReadSpoutTest {

  private static final String NEXT_LINE = "next line";
  private FileReader reader;
  private TridentFileReadSpout spout;
  private TridentCollector collector;

  @BeforeMethod
  public void setUp() {
    reader = mock(FileReader.class);
    spout = new TridentFileReadSpout(10, reader);
    collector = mock(TridentCollector.class);
    when(reader.nextLine()).thenReturn(NEXT_LINE);
  }

  @Test
  public void testEmitBatch() {
    spout.emitBatch(0L, collector);
    spout.emitBatch(1L, collector);

    verify(collector, times(20)).emit(any(Values.class));

    Map<Long, List<String>> batches = spout.getBatches();
    assertThat(batches)
            .hasSize(2)
            .containsKey(0L)
            .containsKey(1L);
    assertThat(batches.get(0L)).hasSize(10);
    assertThat(batches.get(1L)).hasSize(10);
  }

  @Test
  public void ackShouldRemoveBatch() {
    spout.emitBatch(0L, collector);
    Map<Long, List<String>> batches = spout.getBatches();
    assertThat(batches).containsKey(0L);

    spout.ack(0L);
    assertThat(batches).isEmpty();
  }

}
