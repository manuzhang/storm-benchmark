package storm.benchmark.topology;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Fields;
import org.apache.log4j.Logger;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.metrics.DRPCMetrics;
import storm.benchmark.tools.PageViewGenerator;
import storm.benchmark.trident.operation.Distinct;
import storm.benchmark.trident.operation.Expand;
import storm.benchmark.trident.operation.One;
import storm.benchmark.trident.operation.Print;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.KafkaUtils;
import storm.kafka.StringScheme;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.state.ReadOnlyState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.ReadOnlyMapState;
import storm.trident.testing.MemoryMapState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static storm.benchmark.tools.PageView.Extract;
import static storm.benchmark.tools.PageView.Item;


public class DRPC extends StormBenchmark {

  private static final Logger LOG = Logger.getLogger(DRPC.class);
  public static final String FUNCTION = "reach";
  public static final List<String> ARGS =
          Arrays.asList("foo.com", "foo.news.com", "foo.contact.com");
  public static final String SERVER = "drpc.server";
  public static final String PORT = "drpc.port";
  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "topology.component.spout_num";
  public static final String PAGE_ID = "page";
  public static final String PAGE_NUM = "topology.component.page_bolt_num";
  public static final String VIEW_ID = "view";
  public static final String VIEW_NUM = "topology.component.view_bolt_num";

  private int spoutNum = 4;
  private int pageNum = 8;
  private int viewNum = 8;
  private IPartitionedTridentSpout spout;


  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    String server = (String) options.get(SERVER);
    if (null == server) {
      throw new IllegalArgumentException("must set a drpc server");
    }

    Integer port = (Integer) options.get(PORT);
    if (null == port) {
      throw new IllegalArgumentException("must set a drpc port");
    }

    spoutNum = BenchmarkUtils.getInt(options, SPOUT_NUM, spoutNum);
    pageNum = BenchmarkUtils.getInt(options, PAGE_NUM, pageNum);
    viewNum = BenchmarkUtils.getInt(options, VIEW_NUM, viewNum);

    spout = new TransactionalTridentKafkaSpout(
            KafkaUtils.getTridentKafkaConfig(options, new SchemeAsMultiScheme(new StringScheme())));
    metrics = new DRPCMetrics(FUNCTION, ARGS, server, port);

    return this;
  }

  @Override
  public IBenchmark buildTopology() {
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
            .each(new Fields("users"), new Expand(), new Fields("user"))
         //   .groupBy(new Fields("user"))
            .stateQuery(userToFollowers, new Fields("user"), new MapGet(), new Fields("followers"))
            .each(new Fields("followers"), new Expand(), new Fields("follower"))
            .groupBy(new Fields("follower"))
            .aggregate(new One(), new Fields("one"))
            .aggregate(new Fields("one"), new Sum(), new Fields("reach"));
    topology = trident.build();
    return this;
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
