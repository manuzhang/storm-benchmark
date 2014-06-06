package storm.benchmark.topology.common;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.testng.annotations.Test;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class WordCountTest {

  @Test
  public void splitSentenceBoltShouldDeclareOutputFields() {
    WordCount.SplitSentence bolt = new WordCount.SplitSentence();
    OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);

    bolt.declareOutputFields(declarer);

    verify(declarer, times(1)).declare(any(Fields.class));
  }

  @Test
  public void splitSentenceBoltShouldEmitEveryWord() {
    final String sentence = "This is a sentence";
    WordCount.SplitSentence bolt = new WordCount.SplitSentence();
    Tuple tuple = mock(Tuple.class);
    BasicOutputCollector collector = mock(BasicOutputCollector.class);

    when(tuple.getString(0)).thenReturn(sentence);

    bolt.execute(tuple, collector);

    verify(collector, times(4)).emit(any(Values.class));

  }

  @Test
  public void countBoltShouldDeclareOutputFields() {
    WordCount.Count bolt = new WordCount.Count();
    OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);

    bolt.declareOutputFields(declarer);

    verify(declarer, times(1)).declare(any(Fields.class));
  }

  @Test
  public void countBoltShouldCountAndEmitNumberOfEveryWord() {
    String[] words = { "word", "word", "count"};
    WordCount.Count bolt = new WordCount.Count();
    Tuple tuple = mock(Tuple.class);
    BasicOutputCollector collector = mock(BasicOutputCollector.class);

    for (String w : words) {
      when(tuple.getString(0)).thenReturn(w);
      bolt.execute(tuple, collector);
      verify(collector, times(1)).emit(any(Values.class));
    }

    assertThat(bolt.counts.get("word")).isEqualTo(2);
    assertThat(bolt.counts.get("count")).isEqualTo(1);
  }

}
