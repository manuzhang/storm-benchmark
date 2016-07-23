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
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import storm.benchmark.tools.PageViewGenerator;

import java.util.Map;

public class PageViewSpout extends BaseRichSpout {

  public static final String FIELDS = "page_view";

  private SpoutOutputCollector collector;
  private final PageViewGenerator generator;
  private final boolean ackEnabled;
  private long count = 0;

  public PageViewSpout(boolean ackEnabled) {
    this.ackEnabled = ackEnabled;
    this.generator = new PageViewGenerator();
  }

  public PageViewSpout(boolean ackEnabled, PageViewGenerator generator) {
    this.ackEnabled = ackEnabled;
    this.generator = generator;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS));
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void nextTuple() {
    if (ackEnabled) {
      collector.emit(new Values(generator.getNextClickEvent()), count);
      count++;
    } else {
      collector.emit(new Values(generator.getNextClickEvent()));
    }
  }
}
