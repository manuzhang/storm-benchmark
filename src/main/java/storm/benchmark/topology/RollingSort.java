package storm.benchmark.topology;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.bolt.RollingCountBolt;
import storm.benchmark.metrics.BasicMetrics;
import storm.benchmark.spout.FileReadSpout;
import storm.benchmark.util.BenchmarkUtils;

import java.util.Map;

public class RollingSort extends StormBenchmark {

  private static final String RANK_NUM = "rangkings.number";
  private static final String WINDOW_LENGTH = "window.length";
  private static final String EMIT_FREQ = "emit.frequency";

  private static final String SPOUT = "wordGenerator";
  private static final String COUNTER = "counter";
  private static final String IR = "intermediateRanker";
  private static final String TR = "finalRanker";

  // number of spoutNum to run in parallel
  protected int spoutNum = 4;

  // number of rolling count bolts to run in parallel
  protected int rcBoltNum = 8;

  // window length in seconds
  protected int winLen = 9;

  // emit frequency in seconds
  protected int emitFreq = 3;


  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    spoutNum = BenchmarkUtils.getInt(options, SPOUT, spoutNum);
    rcBoltNum = BenchmarkUtils.getInt(options, COUNTER, rcBoltNum);
    winLen = BenchmarkUtils.getInt(options, WINDOW_LENGTH, winLen);
    emitFreq = BenchmarkUtils.getInt(options, EMIT_FREQ, emitFreq);

    metrics = new BasicMetrics();

    return this;
  }

  @Override
  public IBenchmark buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(SPOUT, new FileReadSpout(), spoutNum);
    builder.setBolt(COUNTER, new RollingCountBolt(winLen, emitFreq), rcBoltNum).fieldsGrouping(SPOUT, new Fields("word"));
    topology = builder.createTopology();
    return this;
  }
}
