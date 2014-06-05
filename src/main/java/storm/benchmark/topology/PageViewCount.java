package storm.benchmark.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.benchmark.StormBenchmark;
import storm.benchmark.component.bolt.PageViewBolt;
import storm.benchmark.topology.common.WordCount;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.KafkaUtils;
import storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;

import static storm.benchmark.tools.PageView.Item;

public class PageViewCount extends StormBenchmark {
  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "topology.component.spout_num";
  public static final String VIEW_ID = "view";
  public static final String VIEW_NUM = "topology.component.view_bolt_num";
  public static final String COUNT_ID = "count";
  public static final String COUNT_NUM = "topology.component.count_bolt_num";

  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_VIEW_BOLT_NUM = 4;
  public static final int DEFAULT_COUNT_BOLT_NUM = 4;

  private IRichSpout spout;

  @Override
  public StormTopology getTopology(Config config) {

    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int viewBoltNum = BenchmarkUtils.getInt(config, VIEW_NUM, DEFAULT_VIEW_BOLT_NUM);
    final int cntBoltNum = BenchmarkUtils.getInt(config, COUNT_NUM, DEFAULT_COUNT_BOLT_NUM);

    spout = new KafkaSpout(KafkaUtils.getSpoutConfig(
            config, new SchemeAsMultiScheme(new StringScheme())));

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(VIEW_ID, new PageViewBolt(Item.URL, Item.ONE), viewBoltNum)
           .localOrShuffleGrouping(SPOUT_ID);
    builder.setBolt(COUNT_ID, new WordCount.Count(), cntBoltNum)
            .fieldsGrouping(VIEW_ID, new Fields(Item.URL.toString()));
    return builder.createTopology();
  }
}
