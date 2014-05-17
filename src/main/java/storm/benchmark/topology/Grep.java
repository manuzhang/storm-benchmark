package storm.benchmark.topology;

import backtype.storm.metric.api.CountMetric;
import backtype.storm.spout.SchemeAsMultiScheme;
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
import storm.benchmark.util.Util;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Grep extends StormBenchmark {
  private static final String WORD = "word";
  private static final String COUNT = "count";

  private static final String SPOUT = "spout";
  private static final String FM = "find";
  private static final String CM = "count";

  // pattern string to grep
  protected String ptnString = "string";
  // number of spouts to run in parallel
  protected int spouts = 5;
  // number of matching bolts to run in parallel
  protected int matBolts = 8;
  // number of count bolts to run in parallel
  protected int cntBolts = 4;

  protected SpoutConfig spoutConfig;

  @Override
  public IBenchmark parseOptions(Map options) {
    options.put("patternString", ptnString);
    super.parseOptions(options);

    spouts = Util.retIfPositive(spouts, (Integer) options.get(SPOUT));
    matBolts = Util.retIfPositive(matBolts, (Integer) options.get(FM));
    cntBolts = Util.retIfPositive(cntBolts, (Integer) options.get(CM));

    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    metrics = new BasicMetrics();

    return this;
  }
  @Override
  public IBenchmark buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT, new KafkaSpout(spoutConfig), spouts);
    builder.setBolt(FM, new FindMatchingSentence(), matBolts)
            .shuffleGrouping(SPOUT);
    builder.setBolt(CM, new CountMatchingSentence(), cntBolts)
            .fieldsGrouping(FM, new Fields(WORD));

    topology = builder.createTopology();

    return this;
  }

  public static class FindMatchingSentence extends BaseBasicBolt {
    Pattern pattern;
    Matcher matcher;
    transient CountMetric metric;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
      String patternString = stormConf.get("patternString").toString();
      pattern = Pattern.compile(patternString);
      initMetrics(context);
    }

    private void initMetrics(TopologyContext context) {
      metric = new CountMetric();
      context.registerMetric("matching sentence", metric, 1);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
      matcher = pattern.matcher(input.getString(0));
      if (matcher.find()) {
        collector.emit(new Values(1));
        metric.incr();
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(WORD));
    }
  }

  public static class CountMatchingSentence extends BaseBasicBolt {
    int count = 0;
    transient CountMetric metric;



    @Override
    public void prepare(Map stormConf, TopologyContext context) {
      initMetrics(context);
    }

    private void initMetrics(TopologyContext context) {
      metric = new CountMetric();
      context.registerMetric("count sentence", metric, 1);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
      if (input.getInteger(0).equals(1)) {
        collector.emit(new Values(count++));
        metric.incr();
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(COUNT));
    }
  }
}
