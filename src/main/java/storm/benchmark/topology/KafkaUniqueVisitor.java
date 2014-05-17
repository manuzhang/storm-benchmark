package storm.benchmark.topology;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.bolt.PageViewBolt;
import storm.benchmark.bolt.UniqueVisitorBolt;
import storm.benchmark.util.KafkaUtils;
import storm.benchmark.util.Util;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;

import java.util.Map;

import static storm.benchmark.bolt.PageViewBolt.Item;

public class KafkaUniqueVisitor extends StormBenchmark {
  private final static String SPOUT = "spout";
  private final static String VIEW = "view";
  private final static String UNIQUER = "uniquer";
  private final static String WINDOW_LENGTH = "window_length";
  private final static String EMIT_FREQ = "emit_frequency";

  // number of Spouts to run in parallel
  protected int spoutNum = 4;
  // number of PageViewBolts to run in parallel
  protected int pvBoltNum = 4;
  // number of UniqueVisitorBolts to run in parallel
  protected int uvBoltNum = 4;

  protected int winLen = 60 * 5; // 5 mins
  protected int emitFreq = 60; // 60s

  protected SpoutConfig spoutConfig;

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    spoutNum = Util.retIfPositive(spoutNum, (Integer) options.get(SPOUT));
    pvBoltNum = Util.retIfPositive(pvBoltNum, (Integer) options.get(VIEW));
    uvBoltNum = Util.retIfPositive(uvBoltNum, (Integer) options.get(UNIQUER));
    winLen = Util.retIfPositive(winLen, (Integer) options.get(WINDOW_LENGTH));
    emitFreq = Util.retIfPositive(emitFreq, (Integer) options.get(EMIT_FREQ));
    spoutConfig = KafkaUtils.getKafkaSpoutConfig(options, new SchemeAsMultiScheme(new StringScheme()));
    return this;
  }
  @Override
  public IBenchmark buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT, new KafkaSpout(spoutConfig), spoutNum);
    builder.setBolt(VIEW, new PageViewBolt(Item.URL, Item.USER), pvBoltNum).shuffleGrouping(SPOUT);
    builder.setBolt(UNIQUER, new UniqueVisitorBolt(winLen, emitFreq), uvBoltNum).fieldsGrouping(VIEW, new Fields(Item.URL.toString()));
    topology = builder.createTopology();
    return this;
  }
}
