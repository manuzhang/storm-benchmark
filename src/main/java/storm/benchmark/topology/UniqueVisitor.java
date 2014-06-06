package storm.benchmark.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.benchmark.StormBenchmark;
import storm.benchmark.component.bolt.PageViewBolt;
import storm.benchmark.component.bolt.UniqueVisitorBolt;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.KafkaUtils;
import storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;

import static storm.benchmark.component.spout.pageview.PageView.Item;

public class UniqueVisitor extends StormBenchmark {
  public final static String SPOUT_ID = "spout";
  public final static String SPOUT_NUM = "topology.component.spout_num";
  public final static String VIEW_ID = "view";
  public final static String VIEW_NUM = "topology.component.view_bolt_num";
  public final static String UNIQUER_ID = "uniquer";
  public final static String UNIQUER_NUM = "topology.component.uniquer_bolt_num";
  public final static String WINDOW_LENGTH = "window.length";
  public final static String EMIT_FREQ = "emit.frequency";

  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_PV_BOLT_NUM = 4;
  public static final int DEFAULT_UV_BOLT_NUM = 4;
  public static final int DEFAULT_WINDOW_LENGTH_IN_SEC = 9; //  9s
  public static final int DEFAULT_EMIT_FREQ_IN_SEC = 3; // 3s

  protected IRichSpout spout;

  @Override
  public StormTopology getTopology(Config config) {

    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int pvBoltNum = BenchmarkUtils.getInt(config, VIEW_NUM, DEFAULT_PV_BOLT_NUM);
    final int uvBoltNum = BenchmarkUtils.getInt(config, UNIQUER_NUM, DEFAULT_UV_BOLT_NUM);
    final int winLen = BenchmarkUtils.getInt(config, WINDOW_LENGTH, DEFAULT_WINDOW_LENGTH_IN_SEC);
    final int emitFreq = BenchmarkUtils.getInt(config, EMIT_FREQ, DEFAULT_EMIT_FREQ_IN_SEC);
    spout = new KafkaSpout(KafkaUtils.getSpoutConfig(
            config, new SchemeAsMultiScheme(new StringScheme())));

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(VIEW_ID, new PageViewBolt(Item.URL, Item.USER), pvBoltNum)
            .localOrShuffleGrouping(SPOUT_ID);
    builder.setBolt(UNIQUER_ID, new UniqueVisitorBolt(winLen, emitFreq), uvBoltNum)
            .fieldsGrouping(VIEW_ID, new Fields(Item.URL.toString()));
    return builder.createTopology();
  }
}
