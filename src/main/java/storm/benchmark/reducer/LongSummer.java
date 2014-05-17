package storm.benchmark.reducer;


public class LongSummer implements Reducer<Long> {

  private static final long serialVersionUID = -1102373670176409091L;

  @Override
  public Long reduce(Long v1, Long v2) {
    return v1 + v2;
  }

  @Override
  public Long zero() {
    return (long) 0;
  }

  @Override
  public boolean isZero(Long aLong) {
    return (long) 0 == aLong;
  }
}
