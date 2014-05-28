package storm.benchmark.topology;

import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.bolt.RollingCountBolt;
import storm.benchmark.spout.FileReadSpout;
import storm.benchmark.util.BenchmarkUtils;

import java.util.Map;

public class RollingSort extends StormBenchmark {

  private static final String WINDOW_LENGTH = "window.length";
  private static final String EMIT_FREQ = "emit.frequency";

  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "topology.component.spout_num";
  public static final String COUNTER_ID = "rolling_counter";
  public static final String COUNTER_NUM = "topology.component.bolt_num";

  // number of spoutNum to run in parallel
  private int spoutNum = 4;
  // number of rolling count bolts to run in parallel
  private int rcBoltNum = 8;
  // window length in seconds
  private int winLen = 9;
  // emit frequency in seconds
  private int emitFreq = 3;

  private IRichSpout spout;

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    spoutNum = BenchmarkUtils.getInt(options, SPOUT_NUM, spoutNum);
    rcBoltNum = BenchmarkUtils.getInt(options, COUNTER_NUM, rcBoltNum);
    winLen = BenchmarkUtils.getInt(options, WINDOW_LENGTH, winLen);
    emitFreq = BenchmarkUtils.getInt(options, EMIT_FREQ, emitFreq);

    spout = new FileReadSpout();

    return this;
  }

  @Override
  public IBenchmark buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(COUNTER_ID, new RollingCountBolt(winLen, emitFreq), rcBoltNum).fieldsGrouping(SPOUT_ID, new Fields(FileReadSpout.FIELDS));
    topology = builder.createTopology();
    return this;
  }
}
