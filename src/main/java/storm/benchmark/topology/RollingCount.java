package storm.benchmark.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.benchmark.StormBenchmark;
import storm.benchmark.component.bolt.RollingCountBolt;
import storm.benchmark.component.spout.FileReadSpout;
import storm.benchmark.util.BenchmarkUtils;

public class RollingCount extends StormBenchmark {

  private static final String WINDOW_LENGTH = "window.length";
  private static final String EMIT_FREQ = "emit.frequency";

  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "topology.component.spout_num";
  public static final String COUNTER_ID = "rolling_counter";
  public static final String COUNTER_NUM = "topology.component.bolt_num";

  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_RC_BOLT_NUM = 8;
  public static final int DEFAULT_WINDOW_LENGTH_IN_SECS = 9;   // 9s
  public static final int DEFAULT_EMIT_FREQ_IN_SECS = 3; // 3s

  private IRichSpout spout;

  @Override
  public StormTopology getTopology(Config config) {

    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int rcBoltNum = BenchmarkUtils.getInt(config, COUNTER_NUM, DEFAULT_RC_BOLT_NUM);
    final int windowLength = BenchmarkUtils.getInt(config, WINDOW_LENGTH, DEFAULT_WINDOW_LENGTH_IN_SECS);
    final int emitFreq = BenchmarkUtils.getInt(config, EMIT_FREQ, DEFAULT_EMIT_FREQ_IN_SECS);

    spout = new FileReadSpout();

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(COUNTER_ID, new RollingCountBolt(windowLength, emitFreq), rcBoltNum).fieldsGrouping(SPOUT_ID, new Fields(FileReadSpout.FIELDS));
    return builder.createTopology();
  }
}
