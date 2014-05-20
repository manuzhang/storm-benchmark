package storm.benchmark.trident.operation;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class One implements CombinerAggregator<Integer> {
  @Override
  public Integer init(TridentTuple tuple) {
    return 1;
  }

  @Override
  public Integer combine(Integer val1, Integer val2) {
    return 1;
  }

  @Override
  public Integer zero() {
    return 1;
  }
}
