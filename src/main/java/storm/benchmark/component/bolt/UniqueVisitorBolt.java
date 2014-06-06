package storm.benchmark.component.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.benchmark.component.bolt.common.RollingBolt;
import storm.benchmark.lib.reducer.SetReducer;
import storm.benchmark.util.SlidingWindow;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UniqueVisitorBolt extends RollingBolt {

  private static final long serialVersionUID = -6518481724698629167L;
  public static final String FIELDS_URL = "url";
  public static final String FIELD_UV = "unique_visitor";

  private final SlidingWindow<String, Set<Integer>> window;
  private final Map<String, Set<Integer>> cached;

  public UniqueVisitorBolt(int winLen, int emitFreq) {
    super(winLen, emitFreq);
    window = new SlidingWindow<String, Set<Integer>>(new SetReducer<Integer>(), getWindowChunks());
    cached = new HashMap<String, Set<Integer>>();
  }

  @Override
  public void emitCurrentWindow(BasicOutputCollector collector) {
    dumpCache();
    Map<String, Set<Integer>> urlToVisitors = window.reduceThenAdvanceWindow();
    for (String url : urlToVisitors.keySet()) {
      collector.emit(new Values(url, urlToVisitors.get(url).size()));
    }
  }

  @Override
  public void updateCurrentWindow(Tuple tuple) {
    String url = tuple.getString(0);
    int userId = tuple.getInteger(1);
    Set<Integer> users = cached.get(url);
    if (null == users) {
      users = new HashSet<Integer>();
      cached.put(url, users);
    }
    users.add(userId);
  }

  private void dumpCache() {
    for (String url : cached.keySet()) {
      window.add(url, cached.get(url));
    }
    cached.clear();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS_URL, FIELD_UV));
  }


}
