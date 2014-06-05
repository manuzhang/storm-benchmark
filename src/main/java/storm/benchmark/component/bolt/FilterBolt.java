package storm.benchmark.component.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FilterBolt<T> extends BaseBasicBolt {
  private static final long serialVersionUID = -4957635695743420459L;
  public static final String FIELDS = "filtered";
  private final T toFilter;

  public FilterBolt(T toFilter) {
    this.toFilter = toFilter;
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    if (!filter(input, toFilter)) {
      collector.emit(new Values(input.getValue(1)));
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS));
  }

  public static <F> boolean filter(Tuple input, F toFilter) {
    return toFilter.equals((F) input.getValue(0));
  }
}
