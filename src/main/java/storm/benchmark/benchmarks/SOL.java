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
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import com.google.common.collect.Sets;
import storm.benchmark.lib.bolt.ConstBolt;
import storm.benchmark.lib.spout.RandomMessageSpout;
import storm.benchmark.metrics.BasicMetricsCollector;
import storm.benchmark.metrics.IMetricsCollector;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.util.BenchmarkUtils;

import static storm.benchmark.metrics.IMetricsCollector.MetricsItem;

/**
 * forked from https://github.com/yahoo/storm-perf-test
 */

public class SOL extends StormBenchmark {

  public static final String TOPOLOGY_LEVEL = "topology.level";
  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "component.spout_num";
  public static final String BOLT_ID = "bolt";
  public static final String BOLT_NUM = "component.bolt_num";

  public static final int DEFAULT_NUM_LEVELS = 2;
  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_BOLT_NUM = 4;
  private IRichSpout spout;

  @Override
  public StormTopology getTopology(Config config) {
    final int numLevels = BenchmarkUtils.getInt(config, TOPOLOGY_LEVEL, DEFAULT_NUM_LEVELS);
    final int msgSize = BenchmarkUtils.getInt(config, RandomMessageSpout.MESSAGE_SIZE,
            RandomMessageSpout.DEFAULT_MESSAGE_SIZE);
    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int boltNum = BenchmarkUtils.getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);

    spout = new RandomMessageSpout(msgSize, BenchmarkUtils.ifAckEnabled(config));

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(BOLT_ID + 1, new ConstBolt(), boltNum)
        .shuffleGrouping(SPOUT_ID);
    for (int levelNum = 2; levelNum <= numLevels - 1; levelNum++) {
      builder.setBolt(BOLT_ID + levelNum, new ConstBolt(), boltNum)
        .shuffleGrouping(BOLT_ID + (levelNum - 1));
    }
   return builder.createTopology();
  }

  @Override
  public IMetricsCollector getMetricsCollector(Config config, StormTopology topology) {
    return new BasicMetricsCollector(config, topology,
            Sets.newHashSet(MetricsItem.ALL));
  }

}

