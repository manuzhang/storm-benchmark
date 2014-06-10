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

import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class WordSplit extends BaseFunction {

  private static final Logger LOG = Logger.getLogger(WordSplit.class);
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
    LOG.debug("receive sentence: '" + sentence + "'");
    if (sentence != null) {
      for (String word : splitSentence(sentence)) {
        collector.emit(new Values(word));
      }
    }
  }
}
