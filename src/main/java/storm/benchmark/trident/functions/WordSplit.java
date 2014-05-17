package storm.benchmark.trident.functions;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class WordSplit extends BaseFunction {

  private static final long serialVersionUID = -8605358179216330897L;

  public static String[] splitSentence(String sentence) {
    if (sentence != null) {
      return sentence.split("\\s+");
    }
    return null;
  }

  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
    String sentence = tuple.getString(0);
    if (sentence != null) {
      for (String word : splitSentence(sentence)) {
        collector.emit(new Values(word));
      }
    }
  }
}
