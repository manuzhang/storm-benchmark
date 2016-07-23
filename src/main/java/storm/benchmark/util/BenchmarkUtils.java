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

package storm.benchmark.util;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.utils.Utils;

import java.util.Iterator;
import java.util.Map;

public final class BenchmarkUtils {
  private static final Logger LOG = Logger.getLogger(BenchmarkUtils.class);

  private BenchmarkUtils() {
  }

  public static double max(Iterable<Double> iterable) {
    Iterator<Double> iterator = iterable.iterator();
    double max = Double.MIN_VALUE;
    while (iterator.hasNext()) {
      double d = iterator.next();
      if (d > max) {
        max = d;
      }
    }
    return max;
  }

  public static double avg(Iterable<Double> iterable) {
    Iterator<Double> iterator = iterable.iterator();
    double total = 0.0;
    int num = 0;
    while (iterator.hasNext()) {
      total += iterator.next();
      num++;
    }
    return total / num;
  }

  public static void putIfAbsent(Map map, Object key, Object val) {
    if (!map.containsKey(key)) {
      map.put(key, val);
    }
  }

  public static int getInt(Map map, Object key, int def) {
    return Utils.getInt(Utils.get(map, key, def));
  }

  public static boolean ifAckEnabled(Config config) {
    Object ackers = config.get(Config.TOPOLOGY_ACKER_EXECUTORS);
    if (null == ackers) {
      LOG.warn("acker executors are null");
      return false;
    }
    return Utils.getInt(ackers) > 0;
  }}
