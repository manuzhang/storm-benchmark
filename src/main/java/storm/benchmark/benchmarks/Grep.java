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

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.KafkaUtils;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Grep extends StormBenchmark {
  private static final Logger LOG = Logger.getLogger(Grep.class);

  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "component.spout_num";
  public static final String FM_ID = "find";
  public static final String FM_NUM = "component.find_bolt_num";
  public static final String CM_ID = "count";
  public static final String CM_NUM = "component.count_bolt_num";
  public static final String PATTERN_STRING = "pattern_string";

  public static final String DEFAULT_PATTERN_STR = "string";
  public static final int DEFAULT_SPOUT_NUM = 5;
  public static final int DEFAULT_MAT_BOLT_NUM = 8;
  public static final int DEFAULT_CNT_BOLT_NUM = 4;

  private IRichSpout spout;

  @Override
  public StormTopology getTopology(Config config) {

    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int matBoltNum = BenchmarkUtils.getInt(config, FM_NUM, DEFAULT_MAT_BOLT_NUM);
    final int cntBoltNum = BenchmarkUtils.getInt(config, CM_NUM, DEFAULT_CNT_BOLT_NUM);
    final String ptnString = (String) Utils.get(config, PATTERN_STRING, DEFAULT_PATTERN_STR);

    spout = new KafkaSpout(KafkaUtils.getSpoutConfig(config, new SchemeAsMultiScheme(new StringScheme())));

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(FM_ID, new FindMatchingSentence(ptnString), matBoltNum)
            .localOrShuffleGrouping(SPOUT_ID);
    builder.setBolt(CM_ID, new CountMatchingSentence(), cntBoltNum)
            .fieldsGrouping(FM_ID, new Fields(FindMatchingSentence.FIELDS));

    return builder.createTopology();
  }

  public static class FindMatchingSentence extends BaseBasicBolt {
    public static final String FIELDS = "word";
    private Pattern pattern;
    private Matcher matcher;
    private final String ptnString;

    public FindMatchingSentence(String ptnString) {
      this.ptnString = ptnString;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
      pattern = Pattern.compile(ptnString);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
      String sentence = input.getString(0);
      LOG.debug(String.format("find pattern %s in sentence %s", ptnString, sentence));
      matcher = pattern.matcher(input.getString(0));
      if (matcher.find()) {
        collector.emit(new Values(1));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(FIELDS));
    }
  }

  public static class CountMatchingSentence extends BaseBasicBolt {
    public static final String FIELDS = "count";
    private int count = 0;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
      if (input.getInteger(0).equals(1)) {
        collector.emit(new Values(count++));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(FIELDS));
    }
  }
}
