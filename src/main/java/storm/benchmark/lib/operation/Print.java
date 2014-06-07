package storm.benchmark.lib.operation;

import org.apache.log4j.Logger;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;


/**
 * for debug purpose
 */
public class Print extends BaseFunction {

  private static final Logger LOG = Logger.getLogger(Print.class);
  private final String fields;

  public Print(String fields) {
    this.fields = fields;
  }

  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
    System.out.println("Print " + fields + ": " + tuple.toString());
    collector.emit(tuple);
  }
}
