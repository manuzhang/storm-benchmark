package storm.benchmark.trident.operation;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public class ExpandList extends BaseFunction {

  private static final long serialVersionUID = -8912233679276693760L;

  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
    List l = (List) tuple.getValue(0);
    if (l != null) {
      for (Object o : l) {
        collector.emit(new Values(o));
      }
    }
  }
}
