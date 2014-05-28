package storm.benchmark.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import storm.benchmark.tools.FileReader;

import java.util.Map;

public class FileReadSpout extends BaseRichSpout {
  private static final Logger LOG = Logger.getLogger(FileReadSpout.class);
  private static final long serialVersionUID = -2582705611472467172L;

	public static final String DEFAULT_FILE = "/resources/A_Tale_of_Two_City.txt";
  public static final boolean DEFAULT_ACK = false;
  public static final String FIELDS = "sentence";

  public final boolean ackEnabled;
  public final FileReader reader;
	private SpoutOutputCollector collector;

  private long count = 0;


  public FileReadSpout() {
    this(DEFAULT_ACK, DEFAULT_FILE);
  }


  public FileReadSpout(boolean ackEnabled) {
    this(ackEnabled, DEFAULT_FILE);
  }

  public FileReadSpout(boolean ackEnabled, String file) {
    this.ackEnabled = ackEnabled;
    this.reader = new FileReader(file);
  }

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
    if (ackEnabled) {
      collector.emit(new Values(reader.nextLine()), count);
    } else {
      collector.emit(new Values(reader.nextLine()));
    }
    count++;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FIELDS));
	}
}
