package storm.benchmark.topology;

import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.bolt.IntermediateRankingsBolt;
import storm.benchmark.bolt.RollingCountBolt;
import storm.benchmark.bolt.TotalRankingsBolt;
import storm.benchmark.metrics.BasicMetrics;
import storm.benchmark.util.Util;

import java.util.Map;

/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter.
 *
 * please refer to http://www.michael-noll.com/blog/2013/01/18/implementing-real-time-trending-topics-in-storm/
 */
public class RollingTopWords extends StormBenchmark {

  private static final String RANK_NUM = "rangkings.number";
  private static final String WINDOW_LENGTH = "window.length";
  private static final String EMIT_FREQ = "emit.frequency";

  private static final String SPOUT = "wordGenerator";
  private static final String COUNTER = "counter";
  private static final String IR = "intermediateRanker";
  private static final String TR = "finalRanker";

  // top n of ranking to return
  protected int topN = 5;

  // number of spouts to run in parallel
  protected int spouts = 4;

  // number of rolling count bolts to run in parallel
  protected int rcBolts = 8;

  // number of intermediate ranking bolts to run in parallel
  protected int irBolts = 4;

  // window length in seconds
  protected int winLen = 9;

  // emit frequency in seconds
  protected int emitFreq = 3;


  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    spouts = Util.retIfPositive(spouts, (Integer) options.get(SPOUT));
    rcBolts = Util.retIfPositive(rcBolts, (Integer) options.get(COUNTER));
    irBolts = Util.retIfPositive(irBolts, (Integer) options.get(IR));
    topN = Util.retIfPositive(topN, (Integer) options.get(RANK_NUM));
    winLen = Util.retIfPositive(winLen, (Integer) options.get(WINDOW_LENGTH));
    emitFreq = Util.retIfPositive(emitFreq, (Integer)options.get(EMIT_FREQ));

    metrics = new BasicMetrics();

    return this;
  }

  @Override
  public IBenchmark buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(SPOUT, new TestWordSpout(), spouts);
    builder.setBolt(COUNTER, new RollingCountBolt(winLen, emitFreq), rcBolts).fieldsGrouping(SPOUT, new Fields("word"));
    builder.setBolt(IR, new IntermediateRankingsBolt(topN), irBolts).fieldsGrouping(COUNTER, new Fields(
            "obj"));
    builder.setBolt(TR, new TotalRankingsBolt(topN)).globalGrouping(IR);
    topology = builder.createTopology();
    return this;
  }
}
