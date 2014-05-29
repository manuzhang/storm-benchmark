package storm.benchmark.topology;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.bolt.PageViewBolt;
import storm.benchmark.topology.common.WordCount;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.KafkaUtils;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;

import java.util.Map;

import static storm.benchmark.tools.PageView.Item;

public class PageView extends StormBenchmark {
  private static final String SPOUT = "spout";
  private static final String VIEW = "view";
  private static final String COUNT = "count";

  private SpoutConfig spoutConfig;
  private int spoutNum = 4;
  private int viewBoltNum = 4;
  private int cntBoltNum = 4;

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    spoutNum = BenchmarkUtils.getInt(options, SPOUT, spoutNum);
    viewBoltNum = BenchmarkUtils.getInt(options, VIEW, viewBoltNum);
    cntBoltNum = BenchmarkUtils.getInt(options, COUNT, cntBoltNum);

    spoutConfig = KafkaUtils.getSpoutConfig(options, new SchemeAsMultiScheme(new StringScheme()));

    return this;
  }


  @Override
  public IBenchmark buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT, new KafkaSpout(spoutConfig), spoutNum);
    builder.setBolt(VIEW, new PageViewBolt(Item.URL, Item.ONE), viewBoltNum)
           .localOrShuffleGrouping(SPOUT);
    builder.setBolt(COUNT, new WordCount.Count(), cntBoltNum)
            .fieldsGrouping(VIEW, new Fields(Item.URL.toString()));
    topology = builder.createTopology();
    return this;
  }
}
