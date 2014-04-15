package storm.benchmark.topology;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.metrics.DRPCMetrics;
import storm.benchmark.util.Util;

import java.util.*;

/**
 * This is a good example of doing complex Distributed RPC on top of Storm. This program creates a topology that can
 * compute the reach for any URL on Twitter in realtime by parallelizing the whole computation.
 * <p/>
 * Reach is the number of unique people exposed to a URL on Twitter. To compute reach, you have to get all the people
 * who tweeted the URL, get all the followers of all those people, unique that set of followers, and then count the
 * unique set. It's an intense computation that can involve thousands of database calls and tens of millions of follower
 * records.
 * <p/>
 * This Storm topology does every piece of that computation in parallel, turning what would be a computation that takes
 * minutes on a single machine into one that takes just a couple seconds.
 * <p/>
 * For the purposes of demonstration, this topology replaces the use of actual DBs with in-memory hashmaps.
 * <p/>
 * See https://github.com/nathanmarz/storm/wiki/Distributed-RPC for more information on Distributed RPC.
 */
public class Reach extends StormBenchmark {

  private static final String FUNCTION = "reach";
  private static final String REACH = "reach";
  private static final String ID = "id";
  private static final String FOLLOWER = "follower";
  private static final String TWEETER = "tweeter";
  private static final String PARTIAL_COUNT = "partial-count";


  public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {{
    put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan"));
    put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan"));
    put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john"));
  }};
  public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {{
    put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
    put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
    put("tim", Arrays.asList("alex"));
    put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
    put("adam", Arrays.asList("david", "carissa"));
    put("mike", Arrays.asList("john", "bob"));
    put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
  }};


  // "number of tweeter bolts to run in parallel
  protected int twtBolts = 3;
  // number of follower bolts to run in parallel
  protected int foBolts = 3;
  // number of follower bolts to run in parallel
  protected int uniBolts = 3;
  // number of count bolts to run in parallel
  protected int cntBolts = 3;

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);
    final String tweeterId = "tweeter";
    final String followerId = "follower";
    final String uniquerId = "uniquer";
    final String countId = "count";

    twtBolts = Util.retIfPositive(twtBolts, (Integer) options.get(tweeterId));
    foBolts = Util.retIfPositive(foBolts, (Integer) options.get(followerId));
    uniBolts = Util.retIfPositive(uniBolts, (Integer) options.get(uniquerId));
    cntBolts = Util.retIfPositive(cntBolts, (Integer) options.get(countId));

    metrics = new DRPCMetrics();

    return this;
  }

  @Override
  public IBenchmark buildTopology() {


    LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(FUNCTION);
    builder.addBolt(new GetTweeters(), twtBolts);
    builder.addBolt(new GetFollowers(), foBolts).shuffleGrouping();
    builder.addBolt(new PartialUniquer(), uniBolts).fieldsGrouping(new Fields(ID, FOLLOWER));
    builder.addBolt(new CountAggregator(), cntBolts).fieldsGrouping(new Fields(ID));

    topology = builder.createRemoteTopology();

    return this;
 }

  public static class GetTweeters extends BaseBasicBolt {

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      Object id = tuple.getValue(0);
      String url = tuple.getString(1);
      List<String> tweeters = TWEETERS_DB.get(url);
      if (tweeters != null) {
        for (String tweeter : tweeters) {
          collector.emit(new Values(id, tweeter));
        }
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(ID, TWEETER));
    }
  }

  public static class GetFollowers extends BaseBasicBolt {

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      Object id = tuple.getValue(0);
      String tweeter = tuple.getString(1);
      List<String> followers = FOLLOWERS_DB.get(tweeter);
      if (followers != null) {
        for (String follower : followers) {
          collector.emit(new Values(id, follower));
        }
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(ID, FOLLOWER));
    }
  }

  public static class PartialUniquer extends BaseBatchBolt {
    BatchOutputCollector collector;
    Object id;
    Set<String> followers = new HashSet<String>();

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
      this.collector = collector;
      this.id = id;
    }

    @Override
    public void execute(Tuple tuple) {
      followers.add(tuple.getString(1));
    }

    @Override
    public void finishBatch() {
      collector.emit(new Values(id, followers.size()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(ID, PARTIAL_COUNT));
    }
  }

  public static class CountAggregator extends BaseBatchBolt {
    BatchOutputCollector collector;
    Object id;
    int count = 0;


    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
      this.collector = collector;
      this.id = id;
    }

    @Override
    public void execute(Tuple tuple) {
      count += tuple.getInteger(1);
    }

    @Override
    public void finishBatch() {
      collector.emit(new Values(id, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(ID, REACH));
    }
  }
}
