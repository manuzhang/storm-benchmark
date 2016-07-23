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
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import storm.benchmark.lib.spout.pageview.PageView;

import static storm.benchmark.lib.spout.pageview.PageView.Item;

/**
 * the incoming tuple has a single pageview field consisting
 * of URL, (http) STATUS, ZIP and USER.
 * the outgoing tuple is composed of two fields, where each field
 * could be one of the above, the pageview itself or a count ONE.
 * This is flexible enough to feed downstream bolts for such use cases as
 * PageViewCount(URL, ONE) and UniqueVisitorCount(URL, USER)
 *
 */

public class PageViewBolt extends BaseBasicBolt {

  private static final long serialVersionUID = -523726932372993856L;
  public final Item field1;
  public final Item field2;

  public PageViewBolt(Item field1, Item field2) {
    this.field1 = field1;
    this.field2 = field2;
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    PageView view = PageView.fromString(input.getString(0));
    collector.emit(new Values(view.getValue(field1), view.getValue(field2)));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(field1.toString(), field2.toString()));
  }

}
