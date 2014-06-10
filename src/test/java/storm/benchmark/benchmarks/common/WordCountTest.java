/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package storm.benchmark.benchmarks.common;

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
    }
    verify(collector, times(3)).emit(any(Values.class));

    assertThat(bolt.counts.get("word")).isEqualTo(2);
    assertThat(bolt.counts.get("count")).isEqualTo(1);
  }

}
