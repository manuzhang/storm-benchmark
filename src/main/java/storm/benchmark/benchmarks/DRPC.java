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
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.spout.IPartitionedTridentSpout;
import org.apache.storm.trident.state.ReadOnlyState;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.map.ReadOnlyMapState;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.lib.operation.Distinct;
import storm.benchmark.lib.operation.Expand;
import storm.benchmark.lib.operation.One;
import storm.benchmark.lib.operation.Print;
import storm.benchmark.metrics.DRPCMetricsCollector;
import storm.benchmark.metrics.IMetricsCollector;
import storm.benchmark.tools.PageViewGenerator;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.KafkaUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static storm.benchmark.lib.spout.pageview.PageView.Extract;
import static storm.benchmark.lib.spout.pageview.PageView.Item;


public class DRPC extends StormBenchmark {

  private static final Logger LOG = Logger.getLogger(DRPC.class);
  public static final String FUNCTION = "reach";
  public static final List<String> ARGS =
          Arrays.asList("foo.com", "foo.news.com", "foo.contact.com");
  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "component.spout_num";
  public static final String PAGE_ID = "page";
  public static final String PAGE_NUM = "component.page_bolt_num";
  public static final String VIEW_ID = "view";
  public static final String VIEW_NUM = "component.view_bolt_num";
  public static final String USER_NUM = "component.user_bolt_num";
  public static final String FOLLOWER_NUM = "component.follower_bolt_num";

  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_PAGE_BOLT_NUM = 8;
  public static final int DEFAULT_VIEW_BOLT_NUM = 8;
  public static final int DEFAULT_USER_BOLT_NUM = 4;
  public static final int DEFAULT_FOLLOWER_BOLT_NUM = 4;

  private IPartitionedTridentSpout spout;

  @Override
  public StormTopology getTopology(Config config) {

    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int pageNum = BenchmarkUtils.getInt(config, PAGE_NUM, DEFAULT_PAGE_BOLT_NUM);
    final int viewNum = BenchmarkUtils.getInt(config, VIEW_NUM, DEFAULT_VIEW_BOLT_NUM);
    final int userNum = BenchmarkUtils.getInt(config, USER_NUM, DEFAULT_USER_BOLT_NUM);
    final int followerNum = BenchmarkUtils.getInt(config, FOLLOWER_NUM, DEFAULT_FOLLOWER_BOLT_NUM);

    spout = new TransactionalTridentKafkaSpout(
            KafkaUtils.getTridentKafkaConfig(config, new SchemeAsMultiScheme(new StringScheme())));

    TridentTopology trident = new TridentTopology();
    TridentState urlToUsers =
            trident.newStream("drpc", spout).parallelismHint(spoutNum).shuffle()
            .each(new Fields(StringScheme.STRING_SCHEME_KEY), new Extract(Arrays.asList(Item.URL, Item.USER)),
                    new Fields("url", "user")).parallelismHint(pageNum)
            .groupBy(new Fields("url"))
            .persistentAggregate(new MemoryMapState.Factory(), new Fields("url", "user"), new Distinct(), new Fields("user_set"))
            .parallelismHint(viewNum);
/** debug
 *  1. this proves that the aggregated result has successfully persisted
    urlToUsers.newValuesStream()
            .each(new Fields("url", "user_set"), new Print("(url, user_set)"), new Fields("url2", "user_set2"));
 */
    PageViewGenerator generator = new PageViewGenerator();
    TridentState userToFollowers = trident.newStaticState(new StaticSingleKeyMapState.Factory(generator.genFollowersDB()));
/** debug
  * 2. this proves that MemoryMapState could be read correctly
   trident.newStream("urlToUsers", new PageViewSpout(false))
            .each(new Fields("page_view"), new Extract(Arrays.asList(Item.URL)), new Fields("url"))
            .each(new Fields("url"), new Print("url"), new Fields("url2"))
            .groupBy(new Fields("url2"))
            .stateQuery(urlToUsers, new Fields("url2"),  new MapGet(), new Fields("users"))
            .each(new Fields("users"), new Print("users"), new Fields("users2"));
*/
/** debug
 *  3. this proves that StaticSingleKeyMapState could be read correctly
    trident.newStream("userToFollowers", new PageViewSpout(false))
            .each(new Fields("page_view"), new Extract(Arrays.asList(Item.USER)), new Fields("user"))
            .each(new Fields("user"), new Print("user"), new Fields("user2"))
            .stateQuery(userToFollowers, new Fields("user2"), new MapGet(), new Fields("followers"))
            .each(new Fields("followers"), new Print("followers"), new Fields("followers2"));
 */
    trident.newDRPCStream(FUNCTION, null)
            .each(new Fields("args"), new Print("args"), new Fields("url"))
            .groupBy(new Fields("url"))
            .stateQuery(urlToUsers, new Fields("url"), new MapGet(), new Fields("users"))
            .each(new Fields("users"), new Expand(), new Fields("user")).parallelismHint(userNum)
            .groupBy(new Fields("user"))
            .stateQuery(userToFollowers, new Fields("user"), new MapGet(), new Fields("followers"))
            .each(new Fields("followers"), new Expand(), new Fields("follower")).parallelismHint(followerNum)
            .groupBy(new Fields("follower"))
            .aggregate(new One(), new Fields("one"))
            .aggregate(new Fields("one"), new Sum(), new Fields("reach"));
    return trident.build();
  }

  @Override
  public IMetricsCollector getMetricsCollector(Config config, StormTopology topology) {
    return new DRPCMetricsCollector(config, FUNCTION, ARGS);
  }

  public static class StaticSingleKeyMapState extends ReadOnlyState implements ReadOnlyMapState<Object> {
    public static class Factory implements StateFactory {
      Map map;

      public Factory(Map map) {
        this.map = map;
      }

      @Override
      public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return new StaticSingleKeyMapState(map);
      }

    }

    Map map;

    public StaticSingleKeyMapState(Map map) {
      this.map = map;
    }


    @Override
    public List<Object> multiGet(List<List<Object>> keys) {
      List<Object> ret = new ArrayList();
      for (List<Object> key : keys) {
        Object singleKey = key.get(0);
        Object value = map.get(singleKey);
        LOG.debug("get " + value + " for " + singleKey);
        ret.add(value);
      }
      return ret;
    }

  }

}
