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
import backtype.storm.task.TopologyContext;
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

import java.io.Serializable;
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
    final int chunkSize = BenchmarkUtils.getInt(config, SortBolt.CHUNK_SIZE,
            SortBolt.DEFAULT_CHUNK_SIZE);
    final int emitFreq = BenchmarkUtils.getInt(config, SortBolt.EMIT_FREQ,
            SortBolt.DEFAULT_EMIT_FREQ);
    spout = new RandomMessageSpout(msgSize, BenchmarkUtils.ifAckEnabled(config));
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(SORT_BOLT_ID, new SortBolt(emitFreq, chunkSize), boltNum)
            .localOrShuffleGrouping(SPOUT_ID);
    return builder.createTopology();
  }

  public static class SortBolt extends BaseBasicBolt {

    public static final String EMIT_FREQ = "emit.frequency";
    public static final int DEFAULT_EMIT_FREQ = 60;  // 60s
    public static final String CHUNK_SIZE = "chunk.size";
    public static final int DEFAULT_CHUNK_SIZE = 100;
    public static final String FIELDS = "sorted_data";

    private final int emitFrequencyInSeconds;
    private final int chunkSize;
    private int index = 0;
    private MutableComparable[] data;


    public SortBolt(int emitFrequencyInSeconds, int chunkSize) {
      this.emitFrequencyInSeconds = emitFrequencyInSeconds;
      this.chunkSize = chunkSize;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
      this.data = new MutableComparable[this.chunkSize];
      for (int i = 0; i < this.chunkSize; i++) {
        this.data[i] = new MutableComparable();
      }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
      if (TupleHelpers.isTickTuple(tuple)) {
        Arrays.sort(data);
        basicOutputCollector.emit(new Values(data));
        LOG.info("index = " + index);
      } else {
        Object obj = tuple.getValue(0);
        if (obj instanceof Comparable) {
          data[index].set((Comparable) obj);
        } else {
          throw new RuntimeException("tuple value is not a Comparable");
        }
        index = (index + 1 == chunkSize) ? 0 : index + 1;
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

  private static class MutableComparable implements Comparable, Serializable {
    private static final long serialVersionUID = -5417151427431486637L;
    private Comparable c = null;

    public MutableComparable() {

    }

    public MutableComparable(Comparable c) {
      this.c = c;
    }

    public void set(Comparable c) {
      this.c = c;
    }

    public Comparable get() {
      return c;
    }

    @Override
    public int compareTo(Object other) {
      if (other == null) return 1;
      Comparable oc = ((MutableComparable) other).get();
      if (null == c && null == oc) {
        return 0;
      } else if (null == c) {
        return -1;
      } else if (null == oc) {
        return 1;
      } else {
        return c.compareTo(oc);
      }
    }
  }
}
