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

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.spout.IPartitionedTridentSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.lib.operation.WordSplit;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.KafkaUtils;


public class TridentWordCount extends StormBenchmark {

  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "component.spout_num";
  public static final String SPLIT_ID = "split";
  public static final String SPLIT_NUM = "component.split_bolt_num";
  public static final String COUNT_ID = "count";
  public static final String COUNT_NUM = "component.count_bolt_num";

  public static final int DEFAULT_SPOUT_NUM = 8;
  public static final int DEFAULT_SPLIT_BOLT_NUM = 4;
  public static final int DEFAULT_COUNT_BOLT_NUM = 4;

  private IPartitionedTridentSpout spout;

  @Override
  public StormTopology getTopology(Config config) {
    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int splitNum = BenchmarkUtils.getInt(config, SPLIT_NUM, DEFAULT_SPLIT_BOLT_NUM);
    final int countNum = BenchmarkUtils.getInt(config, COUNT_NUM, DEFAULT_COUNT_BOLT_NUM);

    spout  = new TransactionalTridentKafkaSpout(
            KafkaUtils.getTridentKafkaConfig(config, new SchemeAsMultiScheme(new StringScheme())));

    TridentTopology trident = new TridentTopology();

    trident.newStream("wordcount", spout).name("sentence").parallelismHint(spoutNum).shuffle()
            .each(new Fields(StringScheme.STRING_SCHEME_KEY), new WordSplit(), new Fields("word"))
            .parallelismHint(splitNum)
            .groupBy(new Fields("word"))
            .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
            .parallelismHint(countNum);
/*    trident.newStream("wordcount", spout)
      .each(new Fields(StringScheme.STRING_SCHEME_KEY), new WordSplit(), new Fields("word"))
      .groupBy(new Fields("word"))
      .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));*/


    return trident.build();
  }




}
