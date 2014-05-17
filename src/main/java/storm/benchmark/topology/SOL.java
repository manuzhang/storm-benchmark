package storm.benchmark.topology;

import backtype.storm.topology.TopologyBuilder;
import storm.benchmark.BenchmarkConfig;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.bolt.ConstBolt;
import storm.benchmark.metrics.BasicMetrics;
import storm.benchmark.spout.RandomMessageSpout;
import storm.benchmark.util.Util;

import java.util.Map;

/*
 * forked from https://github.com/yahoo/storm-perf-test
 */

public class SOL extends StormBenchmark {

  private int msgSize = 100;
  private int numLevels = 2;
  private int spouts = 4;
  private int bolts = 4;
  private static final String TOPOLOGY_LEVEL = "topology.level";

  private static final String SPOUT = "spout";
  private static final String BOLT = "bolt";

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    numLevels = Util.retIfPositive(numLevels, (Integer) options.get(TOPOLOGY_LEVEL));
    msgSize = Util.retIfPositive(msgSize, (Integer) options.get(BenchmarkConfig.MESSAGE_SIZE));
    spouts = Util.retIfPositive(spouts, (Integer) options.get(SPOUT));
    bolts = Util.retIfPositive(bolts, (Integer) options.get(BOLT));

    metrics = new BasicMetrics();

    return this;
  }

  @Override
  public IBenchmark buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(SPOUT, new RandomMessageSpout(msgSize, config.ifAckEnabled()), spouts);
    builder.setBolt(BOLT + 1, new ConstBolt(), bolts)
      .shuffleGrouping(SPOUT);
    for (int levelNum = 2; levelNum <= numLevels - 1; levelNum++) {
      builder.setBolt(BOLT + levelNum, new ConstBolt(), bolts)
        .shuffleGrouping(BOLT + (levelNum - 1));
    }
    topology = builder.createTopology();

    return this;
  }
}

