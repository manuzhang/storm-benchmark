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

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class RandomMessageSpout extends BaseRichSpout {

  private static final long serialVersionUID = -4100642374496292646L;
  public static final String FIELDS = "message";
  public static final String MESSAGE_SIZE = "message.size";
  public static final int DEFAULT_MESSAGE_SIZE = 100;

  private final int sizeInBytes;
  private long messageCount = 0;
  private SpoutOutputCollector collector;
  private String [] messages = null;
  private final boolean ackEnabled;
  private Random rand = null;

  public RandomMessageSpout(boolean ackEnabled) {
    this(DEFAULT_MESSAGE_SIZE, ackEnabled);
  }

  public RandomMessageSpout(int sizeInBytes, boolean ackEnabled) {
    this.sizeInBytes = sizeInBytes;
    this.ackEnabled = ackEnabled;
  }

  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.rand = new Random();
    this.collector = collector;
    final int differentMessages = 100;
    this.messages = new String[differentMessages];
    for(int i = 0; i < differentMessages; i++) {
      StringBuilder sb = new StringBuilder(sizeInBytes);
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
      messageCount++;
    } else {
      collector.emit(new Values(message));
    }
  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS));
  }
}
