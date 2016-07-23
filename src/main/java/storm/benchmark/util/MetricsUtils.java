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

import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.SpoutStats;
import org.apache.storm.generated.TopologySummary;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MetricsUtils {

  private static final int MB = 1000 * 1000;

  private MetricsUtils() {
  }

  public static void addLatency(Map<String, List<Double>> stats, String id, double lat) {
    List<Double> latList = stats.get(id);
    if (null == latList) {
      latList = new LinkedList<Double>();
      stats.put(id, latList);
    }
    latList.add(lat);
  }


  public static TopologySummary getTopologySummary(ClusterSummary cs, String name) {
    for (TopologySummary ts : cs.get_topologies()) {
      if (name.equals(ts.get_name())) {
        return ts;
      }
    }
    return null;
  }

  public static double getSpoutCompleteLatency(SpoutStats stats, String window, String stream) {
    Map<String, Map<String, Double>> latAll = stats.get_complete_ms_avg();
    if (latAll != null) {
      // latency in a time window
      Map<String, Double> latWin = latAll.get(window);
      if (latWin != null) {
        // latency in a stream
        Double latStr = latWin.get(stream);
        if (latStr != null) {
          return latStr;
        }
      }
    }
    return 0.0;
  }

  public static long getSpoutAcked(SpoutStats stats, String window, String stream) {
    Map<String, Map<String, Long>> ackedAll = stats.get_acked();
    if (ackedAll != null) {
      Map<String, Long> ackedWin = ackedAll.get(window);
      if (ackedWin != null) {
        Long ackedStr = ackedWin.get(stream);
        if (ackedStr != null) {
          return ackedStr;
        }
      }
    }
    return 0;
  }

  public static long getTransferred(ExecutorStats stats, String window, String stream) {
    Map<String, Map<String, Long>> transAll = stats.get_transferred();
    if (transAll != null) {
      Map<String, Long> transWin = transAll.get(window);
      if (transWin != null) {
        Long transStr = transWin.get(stream);
        if (transStr != null) {
          return transStr;
        }
      }
    }
    return 0;
  }

  // messages per second
  public static double getThroughput(double throughputDiff, long timeDiff) {
    return (0 == timeDiff) ? 0.0 : (throughputDiff / (timeDiff / 1000.0));
  }

  // MB per second
  public static double getThroughputMB(double throughputDiff, long timeDiff, int msgSize) {
    return (0 == timeDiff) ? 0.0 : ((throughputDiff * msgSize) / (timeDiff / 1000.0) / MB);
  }
}
