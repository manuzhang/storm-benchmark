package storm.benchmark.topology;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.bolt.PageViewBolt;
import storm.benchmark.bolt.UniqueVisitorBolt;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.KafkaUtils;
import storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;

import java.util.Map;

import static storm.benchmark.tools.PageView.Item;

public class UniqueVisitor extends StormBenchmark {
  public final static String SPOUT_ID = "spout";
  public final static String SPOUT_NUM = "topology.component.spout_num";
  public final static String VIEW_ID = "view";
  public final static String VIEW_NUM = "topology.component.view_bolt_num";
  public final static String UNIQUER_ID = "uniquer";
  public final static String UNIQUER_NUM = "topology.component.uniquer_bolt_num";
  public final static String WINDOW_LENGTH = "window.length";
  public final static String EMIT_FREQ = "emit.frequency";

  // number of Spouts to run in parallel
  protected int spoutNum = 4;
  // number of PageViewBolts to run in parallel
  protected int pvBoltNum = 4;
  // number of UniqueVisitorBolts to run in parallel
  protected int uvBoltNum = 4;

  protected int winLen = 60 * 5; // 5 mins
  protected int emitFreq = 60; // 60s

  protected IRichSpout spout;

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    spoutNum = BenchmarkUtils.getInt(options, SPOUT_NUM, spoutNum);
    pvBoltNum = BenchmarkUtils.getInt(options, VIEW_NUM, pvBoltNum);
    uvBoltNum = BenchmarkUtils.getInt(options, UNIQUER_NUM, uvBoltNum);
    winLen = BenchmarkUtils.getInt(options, WINDOW_LENGTH, winLen);
    emitFreq = BenchmarkUtils.getInt(options, EMIT_FREQ, emitFreq);
    spout = new KafkaSpout(KafkaUtils.getSpoutConfig(
            options, new SchemeAsMultiScheme(new StringScheme())));
    return this;
  }

  @Override
  public IBenchmark buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(VIEW_ID, new PageViewBolt(Item.URL, Item.USER), pvBoltNum)
            .localOrShuffleGrouping(SPOUT_ID);
    builder.setBolt(UNIQUER_ID, new UniqueVisitorBolt(winLen, emitFreq), uvBoltNum)
            .fieldsGrouping(VIEW_ID, new Fields(Item.URL.toString()));
    topology = builder.createTopology();
    return this;
  }
}
