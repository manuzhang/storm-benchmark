package storm.benchmark.lib.operation;

import org.apache.log4j.Logger;
import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

import java.util.HashSet;
import java.util.Set;

public class Distinct implements CombinerAggregator<Set<Integer>> {

  private static final Logger LOG = Logger.getLogger(Distinct.class);
  private static final long serialVersionUID = 7592229830682953885L;

  @Override
  public Set<Integer> init(TridentTuple tuple) {
    LOG.debug("get tuple: " + tuple);
    Set<Integer> singleton = new HashSet<Integer>();
    singleton.add(tuple.getInteger(1));
    return singleton;
  }

  @Override
  public Set<Integer> combine(Set<Integer> val1, Set<Integer> val2) {
    Set<Integer> union = new HashSet<Integer>();
    union.addAll(val1);
    union.addAll(val2);
    return union;
  }

  @Override
  public Set<Integer> zero() {
    return new HashSet<Integer>();
  }


}