package storm.benchmark.topology.common;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import storm.benchmark.StormBenchmark;
import storm.benchmark.trident.operation.WordSplit;
import storm.benchmark.util.BenchmarkUtils;

import java.util.HashMap;
import java.util.Map;

public abstract class WordCount extends StormBenchmark {
  private static final Logger LOG = Logger.getLogger(WordCount.class);

  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "topology.component.spout_num";
  public static final String SPLIT_ID = "split";
  public static final String SPLIT_NUM = "topology.component.split_bolt_num";
  public static final String COUNT_ID = "count";
  public static final String COUNT_NUM = "topology.component.count_bolt_num";
  public static final int DEFAULT_SPOUT_NUM = 8;
  public static final int DEFAULT_SPLIT_BOLT_NUM = 4;
  public static final int DEFAULT_COUNT_BOLT_NUM = 4;

  protected IRichSpout spout;

  @Override
  public StormTopology getTopology(Config config) {

    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int spBoltNum = BenchmarkUtils.getInt(config, SPLIT_NUM, DEFAULT_SPLIT_BOLT_NUM);
    final int cntBoltNum = BenchmarkUtils.getInt(config, COUNT_NUM, DEFAULT_COUNT_BOLT_NUM);

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(SPLIT_ID, new SplitSentence(), spBoltNum).localOrShuffleGrouping(
            SPOUT_ID);
    builder.setBolt(COUNT_ID, new Count(), cntBoltNum).fieldsGrouping(SPLIT_ID,
      new Fields(SplitSentence.FIELDS));

    return builder.createTopology();
  }

  public IRichSpout getSpout() {
    return spout;
  }

  public static class SplitSentence extends BaseBasicBolt {

    public static final String FIELDS = "word";

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
      declarer.declare(new Fields(FIELDS));
    }

  }

  public static class Count extends BaseBasicBolt {
    public static final String FIELDS_WORD = "word";
    public static final String FIELDS_COUNT = "count";

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
      declarer.declare(new Fields(FIELDS_WORD, FIELDS_COUNT));
    }
  }

}
