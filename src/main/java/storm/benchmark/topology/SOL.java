package storm.benchmark.topology;

import backtype.storm.topology.TopologyBuilder;
import storm.benchmark.BenchmarkConfig;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.bolt.ConstBolt;
import storm.benchmark.spout.RandomMessageSpout;
import storm.benchmark.util.BenchmarkUtils;

import java.util.Map;

/**
 * forked from https://github.com/yahoo/storm-perf-test
 */

public class SOL extends StormBenchmark {

  public static final String TOPOLOGY_LEVEL = "topology.level";
  public static final String SPOUT = "topology.component.spout_num";
  public static final String BOLT = "topology.component.bolt_num";
  private int msgSize = 100;
  private int numLevels = 2;
  private int spoutNum = 4;
  private int boltNum = 4;

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    numLevels = BenchmarkUtils.getInt(options, TOPOLOGY_LEVEL, numLevels);
    msgSize = BenchmarkUtils.getInt(options, BenchmarkConfig.MESSAGE_SIZE, msgSize);
    spoutNum = BenchmarkUtils.getInt(options, SPOUT, spoutNum);
    boltNum = BenchmarkUtils.getInt(options, BOLT, boltNum);

    return this;
  }

  @Override
  public IBenchmark buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(SPOUT, new RandomMessageSpout(msgSize, config.ifAckEnabled()), spoutNum);
    builder.setBolt(BOLT + 1, new ConstBolt(), boltNum)
      .localOrShuffleGrouping(SPOUT);
    for (int levelNum = 2; levelNum <= numLevels - 1; levelNum++) {
      builder.setBolt(BOLT + levelNum, new ConstBolt(), boltNum)
        .localOrShuffleGrouping(BOLT + (levelNum - 1));
    }
    topology = builder.createTopology();

    return this;
  }
}

