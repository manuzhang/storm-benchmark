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
import storm.benchmark.lib.reducer.SetReducer;
import storm.benchmark.tools.SlidingWindow;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UniqueVisitorBolt extends RollingBolt {

  private static final long serialVersionUID = -6518481724698629167L;
  public static final String FIELDS_URL = "url";
  public static final String FIELD_UV = "unique_visitor";

  private final SlidingWindow<String, Set<Integer>> window;
  private final Map<String, Set<Integer>> cached;

  public UniqueVisitorBolt() {
    this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
  }

  public UniqueVisitorBolt(int winLen, int emitFreq) {
    super(winLen, emitFreq);
    window = new SlidingWindow<String, Set<Integer>>(new SetReducer<Integer>(), getWindowChunks());
    cached = new HashMap<String, Set<Integer>>();
  }

  @Override
  public void emitCurrentWindow(BasicOutputCollector collector) {
    dumpCache();
    Map<String, Set<Integer>> urlToVisitors = window.reduceThenAdvanceWindow();
    for (String url : urlToVisitors.keySet()) {
      collector.emit(new Values(url, urlToVisitors.get(url).size()));
    }
  }

  @Override
  public void updateCurrentWindow(Tuple tuple) {
    String url = tuple.getString(0);
    int userId = tuple.getInteger(1);
    Set<Integer> users = cached.get(url);
    if (null == users) {
      users = new HashSet<Integer>();
      cached.put(url, users);
    }
    users.add(userId);
  }

  private void dumpCache() {
    for (String url : cached.keySet()) {
      window.add(url, cached.get(url));
    }
    cached.clear();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS_URL, FIELD_UV));
  }


}
