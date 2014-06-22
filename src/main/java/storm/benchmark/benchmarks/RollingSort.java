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

package storm.benchmark.benchmarks;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.lib.spout.RandomMessageSpout;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.TupleHelpers;

import java.util.*;

public class RollingSort extends StormBenchmark {

  private static final Logger LOG = Logger.getLogger(RollingSort.class);

  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "component.spout_num";
  public static final String SORT_BOLT_ID ="sort";
  public static final String SORT_BOLT_NUM = "component.sort_bolt_num";
  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_SORT_BOLT_NUM = 8;

  private IRichSpout spout;

  @Override
  public StormTopology getTopology(Config config) {
    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int boltNum =  BenchmarkUtils.getInt(config, SORT_BOLT_NUM, DEFAULT_SORT_BOLT_NUM);
    final int msgSize = BenchmarkUtils.getInt(config, RandomMessageSpout.MESSAGE_SIZE,
            RandomMessageSpout.DEFAULT_MESSAGE_SIZE);
    final int emitFreq = BenchmarkUtils.getInt(config, SortBolt.EMIT_FREQ,
            SortBolt.DEFAULT_EMIT_FREQ);
    spout = new RandomMessageSpout(msgSize, BenchmarkUtils.ifAckEnabled(config));
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(SORT_BOLT_ID, new SortBolt(emitFreq), boltNum)
            .localOrShuffleGrouping(SPOUT_ID);
    return builder.createTopology();
  }

  public static class SortBolt extends BaseBasicBolt {

    public static final String EMIT_FREQ = "emit.frequency";
    public static final int DEFAULT_EMIT_FREQ = 60;  // 60s

    public static final String FIELDS = "sorted";

    private int emitFrequencyInSeconds;
    private List<Comparable> data = new ArrayList<Comparable>();


    public SortBolt(int emitFrequencyInSeconds) {
      this.emitFrequencyInSeconds = emitFrequencyInSeconds;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
      if (TupleHelpers.isTickTuple(tuple)) {
        LOG.info("data size: " + data.size());
        for (Comparable c : data) {
          basicOutputCollector.emit(new Values(c));
        }
        data.clear();
      } else {
        data.add((Comparable) tuple.getValue(0));
        Collections.sort(data);

      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      outputFieldsDeclarer.declare(new Fields(FIELDS));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      Map<String, Object> conf = new HashMap<String, Object>();
      conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
      return conf;
    }
  }
}
