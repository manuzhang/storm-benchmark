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


import org.apache.storm.Config;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import storm.benchmark.util.TupleHelpers;

import java.util.HashMap;
import java.util.Map;

public abstract class RollingBolt extends BaseBasicBolt {

  private static final long serialVersionUID = 5996017217250945842L;
  public static final int DEFAULT_NUM_WINDOW_CHUNKS = 5;
  public static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = DEFAULT_NUM_WINDOW_CHUNKS * 60;
  public static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / DEFAULT_NUM_WINDOW_CHUNKS;

  protected final int windowLengthInSeconds;
  protected final int emitFrequencyInSeconds;

  public RollingBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
    this.windowLengthInSeconds = windowLengthInSeconds;
    this.emitFrequencyInSeconds = emitFrequencyInSeconds;
  }

  protected int getWindowChunks() {
    return windowLengthInSeconds / emitFrequencyInSeconds;
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    if (TupleHelpers.isTickTuple(tuple)) {
      emitCurrentWindow(collector);
    } else {
      updateCurrentWindow(tuple);
    }
  }

  public abstract void emitCurrentWindow(BasicOutputCollector collector);

  public abstract void updateCurrentWindow(Tuple tuple);

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> conf = new HashMap<String, Object>();
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
    return conf;
  }
}
