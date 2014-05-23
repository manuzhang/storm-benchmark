package storm.benchmark.topology.common;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.trident.operation.WordSplit;
import storm.benchmark.util.BenchmarkUtils;

import java.util.HashMap;
import java.util.Map;

public abstract class WordCount extends StormBenchmark {
  private static final String SPOUT = "spout";
  private static final String SPLIT = "split";
  private static final String COUNT = "count";

  // number of spoutNum to run in parallel
  protected int spoutNum = 8;
  // number of split bolts to run in parallel
  protected int spBoltNum = 4;
  // number of count bolts to run in parallel
  protected int cntBoltNum = 4;

  protected IRichSpout spout;

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    spoutNum = BenchmarkUtils.getInt(options, SPOUT, spoutNum);
    spBoltNum = BenchmarkUtils.getInt(options, SPLIT, spBoltNum);
    cntBoltNum = BenchmarkUtils.getInt(options, COUNT, cntBoltNum);

    return this;
  }

  @Override
  public IBenchmark buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(SPOUT, spout, spoutNum);
    builder.setBolt(SPLIT, new SplitSentence(), spBoltNum).localOrShuffleGrouping(
            SPOUT);
    builder.setBolt(COUNT, new Count(), cntBoltNum).fieldsGrouping(SPLIT,
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
      for (String word : WordSplit.splitSentence(input.getString(0))) {
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
