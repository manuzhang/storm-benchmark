package storm.benchmark.topology;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.bolt.FilterBolt;
import storm.benchmark.bolt.PageViewBolt;
import storm.benchmark.util.BenchmarkUtils;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;

import java.util.Map;

import static storm.benchmark.tools.PageView.Item;

public class DataClean extends StormBenchmark {
  private final static String SPOUT = "spout";
  private final static String VIEW = "view";
  private final static String FILTER = "filter";
  private final static String EMIT_FREQ = "emit_frequency";

  // number of Spouts to run in parallel
  protected int spoutNum = 4;
  // number of PageViewBolts to run in parallel
  protected int pvBoltNum = 4;
  // number of FilterBolts to run in parallel
  protected int filtBoltNum = 4;

  protected int emitFreq = 60; // 60s

  protected SpoutConfig spoutConfig;

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    spoutNum = BenchmarkUtils.getInt(options, SPOUT, spoutNum);
    pvBoltNum = BenchmarkUtils.getInt(options, VIEW, pvBoltNum);
    filtBoltNum = BenchmarkUtils.getInt(options, FILTER, filtBoltNum);
    emitFreq = BenchmarkUtils.getInt(options, EMIT_FREQ, emitFreq);
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

    return this;
  }

  @Override
  public IBenchmark buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT, new KafkaSpout(spoutConfig), spoutNum);
    builder.setBolt(VIEW, new PageViewBolt(Item.STATUS, Item.ALL), pvBoltNum).shuffleGrouping(SPOUT);
    builder.setBolt(FILTER, new FilterBolt<Integer>(404), filtBoltNum).fieldsGrouping(VIEW, new Fields(Item.STATUS.toString()));
    topology = builder.createTopology();
    return this;
  }
}
