package storm.benchmark.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import com.google.common.collect.Sets;
import storm.benchmark.StormBenchmark;
import storm.benchmark.component.bolt.ConstBolt;
import storm.benchmark.component.spout.RandomMessageSpout;
import storm.benchmark.metrics.BasicMetricsCollector;
import storm.benchmark.metrics.IMetricsCollector;
import storm.benchmark.util.BenchmarkUtils;

import static storm.benchmark.metrics.IMetricsCollector.MetricsItem;

/**
 * forked from https://github.com/yahoo/storm-perf-test
 */

public class SOL extends StormBenchmark {

  public static final String TOPOLOGY_LEVEL = "topology.level";
  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "topology.component.spout_num";
  public static final String BOLT_ID = "bolt";
  public static final String BOLT_NUM = "topology.component.bolt_num";

  public static final int DEFAULT_NUM_LEVELS = 2;
  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_BOLT_NUM = 4;
  private IRichSpout spout;

  @Override
  public StormTopology getTopology(Config config) {
    final int numLevels = BenchmarkUtils.getInt(config, TOPOLOGY_LEVEL, DEFAULT_NUM_LEVELS);
    final int msgSize = BenchmarkUtils.getInt(config, RandomMessageSpout.MESSAGE_SIZE,
            RandomMessageSpout.DEFAULT_MESSAGE_SIZE);
    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int boltNum = BenchmarkUtils.getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);

    spout = new RandomMessageSpout(msgSize, ifAckEnabled(config));

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(BOLT_ID + 1, new ConstBolt(), boltNum)
      .localOrShuffleGrouping(SPOUT_ID);
    for (int levelNum = 2; levelNum <= numLevels - 1; levelNum++) {
      builder.setBolt(BOLT_ID + levelNum, new ConstBolt(), boltNum)
        .localOrShuffleGrouping(BOLT_ID + (levelNum - 1));
    }
   return builder.createTopology();
  }

  @Override
  public IMetricsCollector getMetricsCollector(Config config, StormTopology topology) {
    return new BasicMetricsCollector(config, topology,
            Sets.newHashSet(MetricsItem.ALL));
  }

}

