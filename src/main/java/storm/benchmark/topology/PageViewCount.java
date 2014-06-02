package storm.benchmark.topology;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.bolt.PageViewBolt;
import storm.benchmark.topology.common.WordCount;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.KafkaUtils;
import storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;

import java.util.Map;

import static storm.benchmark.tools.PageView.Item;

public class PageViewCount extends StormBenchmark {
  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "topology.component.spout_num";
  public static final String VIEW_ID = "view";
  public static final String VIEW_NUM = "topology.component.view_bolt_num";
  public static final String COUNT_ID = "count";
  public static final String COUNT_NUM = "topology.component.count_bolt_num";

  private int spoutNum = 4;
  private int viewBoltNum = 4;
  private int cntBoltNum = 4;

  private IRichSpout spout;

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    spoutNum = BenchmarkUtils.getInt(options, SPOUT_NUM, spoutNum);
    viewBoltNum = BenchmarkUtils.getInt(options, VIEW_NUM, viewBoltNum);
    cntBoltNum = BenchmarkUtils.getInt(options, COUNT_NUM, cntBoltNum);

    spout = new KafkaSpout(KafkaUtils.getSpoutConfig(
            options, new SchemeAsMultiScheme(new StringScheme())));

    return this;
  }


  @Override
  public IBenchmark buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(VIEW_ID, new PageViewBolt(Item.URL, Item.ONE), viewBoltNum)
           .localOrShuffleGrouping(SPOUT_ID);
    builder.setBolt(COUNT_ID, new WordCount.Count(), cntBoltNum)
            .fieldsGrouping(VIEW_ID, new Fields(Item.URL.toString()));
    topology = builder.createTopology();
    return this;
  }
}
