/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.benchmark.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.benchmark.util.FileUtils;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TridentFileReadSpout implements IBatchSpout {
  private final String FILE = "/resources/A_Tale_of_Two_City.txt";

  private Fields fields;
  private int maxBatchSize;
  private List<String> lines = new ArrayList<String>();
  private HashMap<Long, List<String>> batches = new HashMap<Long, List<String>>();
  private int index = 0;
  private int limit = 0;
  private boolean cycle = false;


  public TridentFileReadSpout(Fields fields, int maxBatchSize) {
    this.fields = fields;
    this.maxBatchSize = maxBatchSize;
  }

  public void setCycle(boolean cycle) {
    cycle = cycle;
  }
    
  @Override
  public void open(Map conf, TopologyContext context) {
    lines = FileUtils.readLines(this.getClass().getResourceAsStream(FILE));
    limit = lines.size();
  }

  @Override
  public void emitBatch(long batchId, TridentCollector collector) {
    List<String> batch = batches.get(batchId);
    if (batch == null) {
      batch = new ArrayList<String>();
      if (index >= limit && cycle) {
        index = 0;
      }
      for (int i = 0; index < limit && i < maxBatchSize; index++, i++) {
        batch.add(lines.get(i));
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
    return fields;
  }
    
}