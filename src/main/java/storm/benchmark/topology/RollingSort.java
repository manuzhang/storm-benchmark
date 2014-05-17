package storm.benchmark.topology;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.bolt.RollingCountBolt;
import storm.benchmark.metrics.BasicMetrics;
import storm.benchmark.spout.FileReadSpout;
import storm.benchmark.util.Util;

import java.util.Map;

public class RollingSort extends StormBenchmark {

  private static final String RANK_NUM = "rangkings.number";
  private static final String WINDOW_LENGTH = "window.length";
  private static final String EMIT_FREQ = "emit.frequency";

  private static final String SPOUT = "wordGenerator";
  private static final String COUNTER = "counter";
  private static final String IR = "intermediateRanker";
  private static final String TR = "finalRanker";

  // number of spouts to run in parallel
  protected int spouts = 4;

  // number of rolling count bolts to run in parallel
  protected int rcBolts = 8;

  // window length in seconds
  protected int winLen = 9;

  // emit frequency in seconds
  protected int emitFreq = 3;


  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    spouts = Util.retIfPositive(spouts, (Integer) options.get(SPOUT));
    rcBolts = Util.retIfPositive(rcBolts, (Integer) options.get(COUNTER));
    winLen = Util.retIfPositive(winLen, (Integer) options.get(WINDOW_LENGTH));
    emitFreq = Util.retIfPositive(emitFreq, (Integer)options.get(EMIT_FREQ));

    metrics = new BasicMetrics();

    return this;
  }

  @Override
  public IBenchmark buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(SPOUT, new FileReadSpout(), spouts);
    builder.setBolt(COUNTER, new RollingCountBolt(winLen, emitFreq), rcBolts).fieldsGrouping(SPOUT, new Fields("word"));
    topology = builder.createTopology();
    return this;
  }
}
