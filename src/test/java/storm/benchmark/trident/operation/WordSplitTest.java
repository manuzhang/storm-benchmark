package storm.benchmark.trident.operation;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;

public class WordSplitTest {

  final String[] ANY_WORDS = { "foo", "bar" };
  final String sep = System.getProperty("line.separator");
  @Test(dataProvider = "getSentenceWithWhiteSpace")
  public void sentenceShouldBeSplittedWithWhitespace(String sentence) {
    String[] rets = WordSplit.splitSentence(sentence);
    assertEquals(ANY_WORDS.length, rets.length);
    for (int i = 0; i < rets.length; i++) {
      assertEquals(ANY_WORDS[i], rets[i]);
    }
  }

  @DataProvider
  private Object[][] getSentenceWithWhiteSpace() {
    return new String[][] {
            {ANY_WORDS[0] + "  " + ANY_WORDS[1]},
            {ANY_WORDS[0] + sep + ANY_WORDS[1]},
            {ANY_WORDS[0] + " " + sep + " " + ANY_WORDS[1]}
    };
  }
}
