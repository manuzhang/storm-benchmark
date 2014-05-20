package storm.benchmark.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.benchmark.tools.PageView;
import static storm.benchmark.tools.PageView.Item;

/**
 * the incoming tuple has a single pageview field consisting
 * of URL, (http) STATUS, ZIP and USER.
 * the outgoing tuple is composed of two fields, where each field
 * could be one of the above, the pageview itself or a count ONE.
 * This is flexible enough to feed downstream bolts for such use cases as
 * PageViewCount(URL, ONE) and UniqueVisitorCount(URL, USER)
 *
 */

public class PageViewBolt extends BaseBasicBolt {

  private static final long serialVersionUID = -523726932372993856L;
  public final Item field1;
  public final Item field2;

  public PageViewBolt(Item field1, Item field2) {
    this.field1 = field1;
    this.field2 = field2;
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    PageView view = PageView.fromString(input.getString(0));
    collector.emit(new Values(view.getValue(field1), view.getValue(field2)));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(field1.toString(), field2.toString()));
  }

}
