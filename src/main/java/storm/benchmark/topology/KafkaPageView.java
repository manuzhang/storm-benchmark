package storm.benchmark.topology;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.bolt.PageViewBolt;
import storm.benchmark.metrics.BasicMetrics;
import storm.benchmark.topology.common.WordCount;
import storm.benchmark.util.KafkaUtils;
import storm.benchmark.util.Util;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;

import java.util.Map;

import static storm.benchmark.bolt.PageViewBolt.Item;

public class KafkaPageView  extends StormBenchmark {
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

    spoutNum = Util.retIfPositive(spoutNum, (Integer) options.get(SPOUT));
    viewBoltNum = Util.retIfPositive(viewBoltNum, (Integer) options.get(VIEW));
    cntBoltNum = Util.retIfPositive(cntBoltNum, (Integer) options.get(COUNT));

    spoutConfig = KafkaUtils.getKafkaSpoutConfig(options, new SchemeAsMultiScheme(new StringScheme()));
    metrics = new BasicMetrics();
    return this;
  }


  @Override
  public IBenchmark buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT, new KafkaSpout(spoutConfig), spoutNum);
    builder.setBolt(VIEW, new PageViewBolt(Item.URL, Item.ONE), viewBoltNum)
            .shuffleGrouping(SPOUT);
    builder.setBolt(COUNT, new WordCount.Count(), cntBoltNum)
            .fieldsGrouping(VIEW, new Fields(Item.URL.toString()));
    topology = builder.createTopology();
    return this;
  }
}
