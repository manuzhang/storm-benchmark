package storm.benchmark.bolt.common;


import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import storm.benchmark.util.TupleHelpers;

import java.util.HashMap;
import java.util.Map;

public abstract class RollingBolt extends BaseBasicBolt {

  private static final long serialVersionUID = 5996017217250945842L;
  public static final int DEFAULT_NUM_WINDOW_CHUNKS = 5;
  public static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = DEFAULT_NUM_WINDOW_CHUNKS * 60;
  public static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / DEFAULT_NUM_WINDOW_CHUNKS;

  private final int windowLengthInSeconds;
  private final int emitFrequencyInSeconds;

  public RollingBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
    this.windowLengthInSeconds = windowLengthInSeconds;
    this.emitFrequencyInSeconds = emitFrequencyInSeconds;
  }

  protected int getWindowChunks() {
    return windowLengthInSeconds / emitFrequencyInSeconds;
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    if (TupleHelpers.isTickTuple(tuple)) {
      emitCurrentWindow(collector);
    } else {
      updateCurrentWindow(tuple);
    }
  }

  public abstract void emitCurrentWindow(BasicOutputCollector collector);

  public abstract void updateCurrentWindow(Tuple tuple);

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> conf = new HashMap<String, Object>();
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
    return conf;
  }
}
