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

package storm.benchmark.metrics;

import com.google.common.collect.Sets;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.PrintWriter;
import java.util.Set;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static storm.benchmark.metrics.IMetricsCollector.MetricsItem;

public class BasicMetricsCollectorTest {

  private static final Config config = new Config();
  private StormTopology topology;

  @BeforeMethod
  public void setUp() {
    topology = mock(StormTopology.class);
  }

  @Test(dataProvider = "getSupervisorMetrics")
  public void testCollectSupervisorStats(Set<MetricsItem> items, boolean expected) {
    BasicMetricsCollector collector = new BasicMetricsCollector(config, topology, items);
    assertThat(collector.collectSupervisorStats).isEqualTo(expected);
  }

  @DataProvider
  private Object[][] getSupervisorMetrics() {
    Set<MetricsItem> allButSupervisorStats = Sets.newHashSet(MetricsItem.values());
    allButSupervisorStats.remove(MetricsItem.ALL);
    allButSupervisorStats.remove(MetricsItem.SUPERVISOR_STATS);
    return new Object[][] {
            { Sets.newHashSet(MetricsItem.SUPERVISOR_STATS), true },
            { Sets.newHashSet(MetricsItem.ALL), true },
            { allButSupervisorStats, false },
            { Sets.newHashSet(MetricsItem.SUPERVISOR_STATS, MetricsItem.TOPOLOGY_STATS), true }
    };
  }

  @Test(dataProvider = "getTopologyMetrics")
  public void testCollectTopologyStats(Set<MetricsItem> items, boolean expected) {
    BasicMetricsCollector collector = new BasicMetricsCollector(config, topology, items);
    assertThat(collector.collectTopologyStats).isEqualTo(expected);
  }

  @DataProvider
  public Object[][] getTopologyMetrics() {
    Set<MetricsItem> allButTopologyStats = Sets.newHashSet(MetricsItem.values());
    allButTopologyStats.remove(MetricsItem.ALL);
    allButTopologyStats.remove(MetricsItem.TOPOLOGY_STATS);
    return new Object[][] {
            { Sets.newHashSet(MetricsItem.TOPOLOGY_STATS), true },
            { Sets.newHashSet(MetricsItem.ALL), true },
            { allButTopologyStats, false },
            { Sets.newHashSet(MetricsItem.TOPOLOGY_STATS, MetricsItem.SPOUT_LATENCY), true }
    };
  }

  @Test(dataProvider = "getExecutorMetrics")
  public void testCollectExecutorStats(Set<MetricsItem> items, boolean expected) {
    BasicMetricsCollector collector = new BasicMetricsCollector(config, topology, items);
    assertThat(collector.collectExecutorStats).isEqualTo(expected);
  }

  @DataProvider
  public Object[][] getExecutorMetrics() {
    return new Object[][] {
            { Sets.newHashSet(MetricsItem.THROUGHPUT, MetricsItem.SUPERVISOR_STATS), true },
            { Sets.newHashSet(MetricsItem.THROUGHPUT_IN_MB, MetricsItem.TOPOLOGY_STATS), true },
            { Sets.newHashSet(MetricsItem.SPOUT_THROUGHPUT), true },
            { Sets.newHashSet(MetricsItem.SPOUT_LATENCY), true },
            { Sets.newHashSet(MetricsItem.SUPERVISOR_STATS, MetricsItem.TOPOLOGY_STATS), false },
            { Sets.newHashSet(MetricsItem.ALL), true},
    };
  }

  @Test(dataProvider = "getThroughputMetrics")
  public void testCollectThroughput(Set<MetricsItem> items, boolean expected) {
    BasicMetricsCollector collector = new BasicMetricsCollector(config, topology, items);
    assertThat(collector.collectThroughput).isEqualTo(expected);
  }

  @DataProvider
  public Object[][] getThroughputMetrics() {
    Set<MetricsItem> allButThroughput = Sets.newHashSet(MetricsItem.values());
    allButThroughput.remove(MetricsItem.ALL);
    allButThroughput.remove(MetricsItem.THROUGHPUT);
    return new Object[][] {
            { Sets.newHashSet(MetricsItem.THROUGHPUT), true },
            { Sets.newHashSet(MetricsItem.ALL), true},
            { allButThroughput, false },
            { Sets.newHashSet(MetricsItem.TOPOLOGY_STATS, MetricsItem.THROUGHPUT), true }
    };
  }

  @Test(dataProvider = "getThroughputMBMetrics")
  public void testCollectThroughputMB(Set<MetricsItem> items, boolean expected) {
    BasicMetricsCollector collector = new BasicMetricsCollector(config, topology, items);
    assertThat(collector.collectThroughputMB).isEqualTo(expected);
  }

  @DataProvider
  public Object[][] getThroughputMBMetrics() {
    Set<MetricsItem> allButThroughputMB = Sets.newHashSet(MetricsItem.values());
    allButThroughputMB.remove(MetricsItem.ALL);
    allButThroughputMB.remove(MetricsItem.THROUGHPUT_IN_MB);
    return new Object[][] {
            { Sets.newHashSet(MetricsItem.THROUGHPUT_IN_MB), true },
            { Sets.newHashSet(MetricsItem.ALL), true},
            { allButThroughputMB, false },
            { Sets.newHashSet(MetricsItem.SUPERVISOR_STATS, MetricsItem.THROUGHPUT_IN_MB), true }
    };
  }

  @Test(dataProvider = "getSpoutThroughputMetrics")
  public void testCollectSpoutThroughput(Set<MetricsItem> items, boolean expected) {
    BasicMetricsCollector collector = new BasicMetricsCollector(config, topology, items);
    assertThat(collector.collectSpoutThroughput).isEqualTo(expected);
  }

  @DataProvider
  public Object[][] getSpoutThroughputMetrics() {
    Set<MetricsItem> allButSpoutThroughput = Sets.newHashSet(MetricsItem.values());
    allButSpoutThroughput.remove(MetricsItem.ALL);
    allButSpoutThroughput.remove(MetricsItem.SPOUT_THROUGHPUT);
    return new Object[][] {
            { Sets.newHashSet(MetricsItem.SPOUT_THROUGHPUT), true },
            { Sets.newHashSet(MetricsItem.ALL), true},
            { allButSpoutThroughput, false },
            { Sets.newHashSet(MetricsItem.THROUGHPUT, MetricsItem.SPOUT_THROUGHPUT), true }
    };
  }

  @Test(dataProvider = "getSpoutLatencyMetrics")
  public void testCollectSpoutLatency(Set<MetricsItem> items, boolean expected) {
    BasicMetricsCollector collector = new BasicMetricsCollector(config, topology, items);
    assertThat(collector.collectSpoutLatency).isEqualTo(expected);
  }

  @DataProvider
  public Object[][] getSpoutLatencyMetrics() {
    Set<MetricsItem> allButSpoutLatency = Sets.newHashSet(MetricsItem.values());
    allButSpoutLatency.remove(MetricsItem.ALL);
    allButSpoutLatency.remove(MetricsItem.SPOUT_LATENCY);
    return new Object[][] {
            { Sets.newHashSet(MetricsItem.SPOUT_LATENCY), true },
            { Sets.newHashSet(MetricsItem.ALL), true},
            { allButSpoutLatency, false },
            { Sets.newHashSet(MetricsItem.THROUGHPUT_IN_MB, MetricsItem.SPOUT_LATENCY), true }
    };

  }


  @Test(dataProvider = "getMetricsItems")
  public void testWriteHeaders(Set<MetricsItem> items, Set<String> expected) {
    BasicMetricsCollector collector = new BasicMetricsCollector(config, topology, items);
    PrintWriter writer = mock(PrintWriter.class);

    collector.writeHeader(writer);

    verify(writer, times(1)).println(anyString());
    verify(writer, times(1)).flush();
    for (String h : expected) {
      collector.header.contains(h);
    }

  }

  @DataProvider
  public Object[][] getMetricsItems() {
    Set<String> supervisor = Sets.newHashSet(
            BasicMetricsCollector.TIME,
            BasicMetricsCollector.TOTAL_SLOTS,
            BasicMetricsCollector.USED_SLOTS);
    Set<String> topology = Sets.newHashSet(
            BasicMetricsCollector.TIME,
            BasicMetricsCollector.EXECUTORS,
            BasicMetricsCollector.WORKERS,
            BasicMetricsCollector.TASKS
    );
    Set<String> throughput = Sets.newHashSet(
            BasicMetricsCollector.TIME,
            BasicMetricsCollector.TRANSFERRED,
            BasicMetricsCollector.THROUGHPUT
    );
    Set<String> throughputMB = Sets.newHashSet(
            BasicMetricsCollector.TIME,
            BasicMetricsCollector.THROUGHPUT_MB,
            BasicMetricsCollector.SPOUT_THROUGHPUT_MB
    );
    Set<String> spoutThroughput = Sets.newHashSet(
            BasicMetricsCollector.TIME,
            BasicMetricsCollector.SPOUT_ACKED,
            BasicMetricsCollector.SPOUT_TRANSFERRED,
            BasicMetricsCollector.SPOUT_EXECUTORS,
            BasicMetricsCollector.SPOUT_THROUGHPUT
    );
    Set<String> spoutLatency = Sets.newHashSet(
            BasicMetricsCollector.TIME,
            BasicMetricsCollector.SPOUT_AVG_COMPLETE_LATENCY,
            BasicMetricsCollector.SPOUT_MAX_COMPLETE_LATENCY
    );
    return new Object[][] {
            { Sets.newHashSet(MetricsItem.SUPERVISOR_STATS), supervisor },
            { Sets.newHashSet(MetricsItem.TOPOLOGY_STATS), topology },
            { Sets.newHashSet(MetricsItem.THROUGHPUT), throughput },
            { Sets.newHashSet(MetricsItem.THROUGHPUT_IN_MB), throughputMB },
            { Sets.newHashSet(MetricsItem.SPOUT_THROUGHPUT), spoutThroughput },
            { Sets.newHashSet(MetricsItem.SPOUT_LATENCY), spoutLatency }
    };
  }

}
