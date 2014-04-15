package storm.benchmark.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.benchmark.util.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileReadSpout extends BaseRichSpout {
	private static final String FILE = "/resources/A_Tale_of_Two_City.txt";

  private boolean ackEnabled = false;
  private String field = "word";
	private List<String> lines = new ArrayList<String>();
	SpoutOutputCollector collector;
	private int index = 0;
	private int limit = 0;
  private long count = 0;


  public FileReadSpout() {
  }

  public FileReadSpout(boolean ackEnabled, String field) {
    this.ackEnabled = ackEnabled;
    this.field = field;
  }

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
    lines = FileUtils.readLines(this.getClass().getResourceAsStream(FILE));
    limit = lines.size();
	}

	@Override
	public void nextTuple() {
	  if (index >= limit) {
	    index = 0;
	  }
    if (ackEnabled) {
      collector.emit(new Values(lines.get(index)), count);
    } else {
      collector.emit(new Values(lines.get(index)));
    }
    index++;
    count++;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(field));
	}
}
