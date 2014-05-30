package storm.benchmark.topology;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.KafkaUtils;
import storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Grep extends StormBenchmark {
  private static final Logger LOG = Logger.getLogger(Grep.class);

  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "topology.component.spout_num";
  public static final String FM_ID = "find";
  public static final String FM_NUM = "topology.component.find_bolt_num";
  public static final String CM_ID = "count";
  public static final String CM_NUM = "topology.component.count_bolt_num";
  public static final String PATTERN_STRING = "pattern_string";

  // pattern string to grep
  private static String ptnString = "string";
  // number of spoutNum to run in parallel
  private int spoutNum = 5;
  // number of matching bolts to run in parallel
  private int matBoltNum = 8;
  // number of count bolts to run in parallel
  private int cntBoltNum = 4;

  private IRichSpout spout;

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    spoutNum = BenchmarkUtils.getInt(options, SPOUT_NUM, spoutNum);
    matBoltNum = BenchmarkUtils.getInt(options, FM_NUM, matBoltNum);
    cntBoltNum = BenchmarkUtils.getInt(options, CM_NUM, cntBoltNum);
    ptnString = (String) Utils.get(options, PATTERN_STRING, ptnString);

    spout = new KafkaSpout(KafkaUtils.getSpoutConfig(options, new SchemeAsMultiScheme(new StringScheme())));

    return this;
  }

  @Override
  public IBenchmark buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(FM_ID, new FindMatchingSentence(), matBoltNum)
            .localOrShuffleGrouping(SPOUT_ID);
    builder.setBolt(CM_ID, new CountMatchingSentence(), cntBoltNum)
            .fieldsGrouping(FM_ID, new Fields(FindMatchingSentence.FIELDS));

    topology = builder.createTopology();

    return this;
  }

  public static class FindMatchingSentence extends BaseBasicBolt {
    public static final String FIELDS = "word";
    private Pattern pattern;
    private Matcher matcher;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
      pattern = Pattern.compile(ptnString);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
      String sentence = input.getString(0);
      LOG.debug(String.format("find pattern %s in sentence %s", ptnString, sentence));
      matcher = pattern.matcher(input.getString(0));
      if (matcher.find()) {
        collector.emit(new Values(1));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(FIELDS));
    }
  }

  public static class CountMatchingSentence extends BaseBasicBolt {
    public static final String FIELDS = "count";
    private int count = 0;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
      if (input.getInteger(0).equals(1)) {
        collector.emit(new Values(count++));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(FIELDS));
    }
  }
}
