/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package storm.benchmark.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.benchmark.util.Util;

import java.util.Map;
import java.util.Random;

public class RandomMessageSpout extends BaseRichSpout {

  private static final long serialVersionUID = -4100642374496292646L;
  private int sizeInBytes;
  private long messageCount;
  private SpoutOutputCollector collector;
  private String [] messages = null;
  private boolean ackEnabled;
  private Random rand = null;

  public RandomMessageSpout(int sizeInBytes, boolean ackEnabled) {
    this.sizeInBytes = Util.retIfPositive(0, sizeInBytes);
    this.messageCount = 0;
    this.ackEnabled = ackEnabled;
  }

  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.rand = new Random();
    this.collector = collector;
    final int differentMessages = 100;
    this.messages = new String[differentMessages];
    for(int i = 0; i < differentMessages; i++) {
      StringBuilder sb = new StringBuilder(sizeInBytes);
      //Even though java encodes strings in UCS2, the serialized version sent by the tuples
      // is UTF8, so it should be a single byte
      for(int j = 0; j < sizeInBytes; j++) {
        sb.append(rand.nextInt(9));
      }
      messages[i] = sb.toString();
    }
  }


  @Override
  public void nextTuple() {
    final String message = messages[rand.nextInt(messages.length)];
    if(ackEnabled) {
      collector.emit(new Values(message), messageCount);
    } else {
      collector.emit(new Values(message));
    }
    messageCount++;
  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("message"));
  }
}
