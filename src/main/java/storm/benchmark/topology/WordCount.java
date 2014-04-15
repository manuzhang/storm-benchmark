package storm.benchmark.topology;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.metrics.BasicMetrics;
import storm.benchmark.spout.FileReadSpout;
import storm.benchmark.util.Util;

import java.util.HashMap;
import java.util.Map;

public class WordCount extends StormBenchmark {
  private static final String SPOUT = "spout";
  private static final String SPLIT = "split";
  private static final String COUNT = "count";

  // number of spouts to run in parallel
  private int spouts = 8;
  // number of split bolts to run in parallel
  private int spBolts = 4;
  // number of count bolts to run in parallel
  private int cntBolts = 4;

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    spouts = Util.retIfPositive(spouts, (Integer) options.get(SPOUT));
    spBolts = Util.retIfPositive(spBolts, (Integer) options.get(SPLIT));
    cntBolts = Util.retIfPositive(cntBolts, (Integer) options.get(COUNT));

    metrics = new BasicMetrics();

    return this;
  }

  @Override
  public IBenchmark buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(SPOUT, new FileReadSpout(config.ifAckEnabled(),  "line"), spouts);
    builder.setBolt(SPLIT, new SplitSentence(), spBolts).localOrShuffleGrouping(
            SPOUT);
    builder.setBolt(COUNT, new Count(), cntBolts).fieldsGrouping(SPLIT,
      new Fields("word"));
    topology = builder.createTopology();

    return this;
  }

  public static class SplitSentence extends BaseBasicBolt {

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
      String line = input.getString(0);
      for (String word : line.split(" ")) {
        collector.emit(new Values(word));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

  }

  public static class Count extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }
}
