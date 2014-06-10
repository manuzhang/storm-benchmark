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
package storm.benchmark.lib.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.benchmark.tools.FileReader;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TridentFileReadSpout implements IBatchSpout {

  private static final long serialVersionUID = -3538746749629409899L;
  private static final String DEFAULT_FILE = "/resources/A_Tale_of_Two_City.txt";
  public static final String FIELDS = "sentence";

  private final int maxBatchSize;
  private final FileReader reader;
  private final HashMap<Long, List<String>> batches = new HashMap<Long, List<String>>();

  public TridentFileReadSpout(int maxBatchSize) {
    this(maxBatchSize, DEFAULT_FILE);
  }

  public TridentFileReadSpout(int maxBatchSize, String file) {
    this.maxBatchSize = maxBatchSize;
    this.reader = new FileReader(file);
  }

  public TridentFileReadSpout(int maxBatchSize, FileReader reader) {
    this.maxBatchSize = maxBatchSize;
    this.reader = reader;
  }

  @Override
  public void open(Map conf, TopologyContext context) {
  }

  @Override
  public void emitBatch(long batchId, TridentCollector collector) {
    List<String> batch = batches.get(batchId);
    if (batch == null) {
      batch = new ArrayList<String>();
      for (int i = 0;  i < maxBatchSize; i++) {
        batch.add(reader.nextLine());
      }
      batches.put(batchId, batch);
    }
    for(String line : batch){
      collector.emit(new Values(line));
    }
  }

  @Override
  public void ack(long batchId) {
    batches.remove(batchId);
  }

  @Override
  public void close() {
  }

  @Override
  public Map getComponentConfiguration() {
    return null;
  }

  @Override
  public Fields getOutputFields() {
    return new Fields(FIELDS);
  }

  Map<Long, List<String>> getBatches() {
    return batches;
  }


}