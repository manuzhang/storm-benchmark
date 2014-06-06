package storm.benchmark.lib.reducer;


public class LongSummer implements Reducer<Long> {

  private static final long serialVersionUID = -1102373670176409091L;

  @Override
  public Long reduce(Long v1, Long v2) {
    return v1 + v2;
  }

  @Override
  public Long zero() {
    return 0L;
  }

  @Override
  public boolean isZero(Long aLong) {
    return 0L == aLong;
  }
}
