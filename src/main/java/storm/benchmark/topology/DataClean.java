package storm.benchmark.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.benchmark.StormBenchmark;
import storm.benchmark.component.bolt.FilterBolt;
import storm.benchmark.component.bolt.PageViewBolt;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.KafkaUtils;
import storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;

import static storm.benchmark.component.spout.pageview.PageView.Item;

public class DataClean extends StormBenchmark {
  public final static String SPOUT_ID = "spout";
  public final static String SPOUT_NUM = "topology.component.spout_num";
  public final static String VIEW_ID = "view";
  public final static String VIEW_NUM = "topology.component.view_bolt_num";
  public final static String FILTER_ID = "filter";
  public final static String FILTER_NUM = "topology.component.filter_bolt_num";

  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_PV_BOLT_NUM = 4;
  public static final int DEFAULT_FITLER_BOLT_NUM = 4;

  private IRichSpout spout;


  @Override
  public StormTopology getTopology(Config config) {
    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int pvBoltNum = BenchmarkUtils.getInt(config, VIEW_NUM, DEFAULT_PV_BOLT_NUM);
    final int filterBoltNum = BenchmarkUtils.getInt(config, FILTER_NUM, DEFAULT_FITLER_BOLT_NUM);
    spout = new KafkaSpout(KafkaUtils.getSpoutConfig(
            config, new SchemeAsMultiScheme(new StringScheme())));

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(VIEW_ID, new PageViewBolt(Item.STATUS, Item.ALL), pvBoltNum)
            .localOrShuffleGrouping(SPOUT_ID);
    builder.setBolt(FILTER_ID, new FilterBolt<Integer>(404), filterBoltNum)
            .fieldsGrouping(VIEW_ID, new Fields(Item.STATUS.toString()));
    return builder.createTopology();
  }
}
