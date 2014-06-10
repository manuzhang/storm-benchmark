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
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.benchmark.lib.bolt.PageViewBolt;
import storm.benchmark.lib.bolt.UniqueVisitorBolt;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.KafkaUtils;
import storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;

import static storm.benchmark.lib.spout.pageview.PageView.Item;

public class UniqueVisitor extends StormBenchmark {
  public final static String SPOUT_ID = "spout";
  public final static String SPOUT_NUM = "benchmarks.component.spout_num";
  public final static String VIEW_ID = "view";
  public final static String VIEW_NUM = "benchmarks.component.view_bolt_num";
  public final static String UNIQUER_ID = "uniquer";
  public final static String UNIQUER_NUM = "benchmarks.component.uniquer_bolt_num";
  public final static String WINDOW_LENGTH = "window.length";
  public final static String EMIT_FREQ = "emit.frequency";

  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_PV_BOLT_NUM = 4;
  public static final int DEFAULT_UV_BOLT_NUM = 4;
  public static final int DEFAULT_WINDOW_LENGTH_IN_SEC = 9; //  9s
  public static final int DEFAULT_EMIT_FREQ_IN_SEC = 3; // 3s

  protected IRichSpout spout;

  @Override
  public StormTopology getTopology(Config config) {

    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int pvBoltNum = BenchmarkUtils.getInt(config, VIEW_NUM, DEFAULT_PV_BOLT_NUM);
    final int uvBoltNum = BenchmarkUtils.getInt(config, UNIQUER_NUM, DEFAULT_UV_BOLT_NUM);
    final int winLen = BenchmarkUtils.getInt(config, WINDOW_LENGTH, DEFAULT_WINDOW_LENGTH_IN_SEC);
    final int emitFreq = BenchmarkUtils.getInt(config, EMIT_FREQ, DEFAULT_EMIT_FREQ_IN_SEC);
    spout = new KafkaSpout(KafkaUtils.getSpoutConfig(
            config, new SchemeAsMultiScheme(new StringScheme())));

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(VIEW_ID, new PageViewBolt(Item.URL, Item.USER), pvBoltNum)
            .localOrShuffleGrouping(SPOUT_ID);
    builder.setBolt(UNIQUER_ID, new UniqueVisitorBolt(winLen, emitFreq), uvBoltNum)
            .fieldsGrouping(VIEW_ID, new Fields(Item.URL.toString()));
    return builder.createTopology();
  }
}
