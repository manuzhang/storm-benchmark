package storm.benchmark.tools;

import java.io.Serializable;

public class Mutable implements Serializable {

  private static final long serialVersionUID = 2018481354727996546L;
  private Object val;

  public Mutable() {
  }

  public Mutable(Object val) {
    this.val = val;
  }

  public void set(Object val) {
    this.val = val;
  }

  public Object get() {
    return val;
  }
}
