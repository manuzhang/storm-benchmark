package storm.benchmark.reducer;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Set;

public class SetReducer<T> implements Reducer<Set<T>> {

  private static final long serialVersionUID = -5236751818829417995L;

  @Override
  public Set<T> reduce(Set<T> v1, Set<T> v2) {
    return Sets.union(v1, v2);
  }

  @Override
  public Set<T> zero() {
    return ImmutableSet.of();
  }

  @Override
  public boolean isZero(Set<T> ts) {
    return ts.isEmpty();
  }
}
