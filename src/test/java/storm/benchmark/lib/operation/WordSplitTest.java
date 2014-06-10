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

package storm.benchmark.lib.operation;

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
