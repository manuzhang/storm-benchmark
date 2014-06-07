package storm.benchmark.lib.reducer;

import java.io.Serializable;

public interface Reducer<V> extends Serializable {
  public V reduce(V v1, V v2);
  public V zero();
  public boolean isZero(V v);
}
