package storm.benchmark.topology;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.metrics.DRPCMetrics;
import storm.benchmark.util.KafkaUtils;
import storm.kafka.StringScheme;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.testing.MemoryMapState;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static storm.benchmark.tools.PageView.Extract;
import static storm.benchmark.tools.PageView.Item;


public class DRPC extends StormBenchmark {

  public static final String FUNCTION = "page_view";
  public static final List<String> ARGS =
          Arrays.asList("http://foo.com/", "http://foo.com/news", "http://foo.com/contact");
  public static final String SERVER = "drpc.server";
  public static final String PORT = "drpc.port";

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

    spout = new TransactionalTridentKafkaSpout(
            KafkaUtils.getTridentKafkaConfig(options, new SchemeAsMultiScheme(new StringScheme())));
    metrics = new DRPCMetrics(FUNCTION, ARGS, server, port);

    return this;
  }

  @Override
  public IBenchmark buildTopology() {
    TridentTopology trident = new TridentTopology();
    TridentState state = trident.newStream("drpc", spout).shuffle()
            .each(new Fields(StringScheme.STRING_SCHEME_KEY), new Extract(Arrays.asList(Item.URL)), new Fields("urls"))
            .groupBy(new Fields("urls"))
            .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("url_counts"));

    trident.newDRPCStream(FUNCTION, null).stateQuery(state, new Fields("args"),  new MapGet(), new Fields("page_views"));
    topology = trident.build();
    return this;
  }

}
