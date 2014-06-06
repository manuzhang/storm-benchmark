package storm.benchmark.lib.operation;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.Iterator;

public class Expand extends BaseFunction {

  private static final long serialVersionUID = -8912233679276693760L;

  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
    Iterable iterable = (Iterable) tuple.getValue(0);
    Iterator iterator = iterable.iterator();
    while (iterator.hasNext()) {
     collector.emit(new Values(iterator.next()));
    }
  }
}
