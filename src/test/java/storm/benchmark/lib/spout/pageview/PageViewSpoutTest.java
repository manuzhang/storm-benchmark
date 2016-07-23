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

package storm.benchmark.lib.spout.pageview;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import storm.benchmark.tools.PageViewGenerator;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PageViewSpoutTest {
  private static final Map ANY_CONF = new HashMap();
  private static final String NEXT_CLICK_EVENT = "next click event";
  private OutputFieldsDeclarer declarer;
  private TopologyContext context;
  private SpoutOutputCollector collector;
  private PageViewGenerator generator;

  @BeforeMethod
  public void setUp() {
    declarer = mock(OutputFieldsDeclarer.class);
    collector = mock(SpoutOutputCollector.class);
    context = mock(TopologyContext.class);
    generator = mock(PageViewGenerator.class);
    when(generator.getNextClickEvent()).thenReturn(NEXT_CLICK_EVENT);
  }

  @Test
  public void shouldDeclareOutputFields() {
    PageViewSpout spout = new PageViewSpout(false, generator);

    spout.declareOutputFields(declarer);

    verify(declarer, times(1)).declare(any(Fields.class));
  }

  @Test
  public void shouldEmitValueAndIdWhenAckEnabled() {
    PageViewSpout spout = new PageViewSpout(true, generator);

    spout.open(ANY_CONF, context, collector);
    spout.nextTuple();

    verify(collector, times(1)).emit(any(Values.class), anyInt());
  }

  @Test
  public void shouldEmitValueOnlyWhenAckDisabled() {
    PageViewSpout spout = new PageViewSpout(false, generator);

    spout.open(ANY_CONF, context, collector);
    spout.nextTuple();

    verify(collector, times(1)).emit(any(Values.class));
  }
}
