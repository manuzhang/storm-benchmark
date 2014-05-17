package storm.benchmark.tools;

import java.io.Serializable;

public class Mutable<T> implements Serializable {

  private static final long serialVersionUID = 2018481354727996546L;
  private T val;

  public Mutable(T val) {
    this.val = val;
  }

  public void set(T val) {
    this.val = val;
  }

  public T get() {
    return val;
  }
}
