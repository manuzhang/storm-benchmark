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

import org.apache.log4j.Logger;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.HashSet;
import java.util.Set;

public class Distinct implements CombinerAggregator<Set<Integer>> {

  private static final Logger LOG = Logger.getLogger(Distinct.class);
  private static final long serialVersionUID = 7592229830682953885L;

  @Override
  public Set<Integer> init(TridentTuple tuple) {
    LOG.debug("get tuple: " + tuple);
    Set<Integer> singleton = new HashSet<Integer>();
    singleton.add(tuple.getInteger(1));
    return singleton;
  }

  @Override
  public Set<Integer> combine(Set<Integer> val1, Set<Integer> val2) {
    Set<Integer> union = new HashSet<Integer>();
    union.addAll(val1);
    union.addAll(val2);
    return union;
  }

  @Override
  public Set<Integer> zero() {
    return new HashSet<Integer>();
  }


}