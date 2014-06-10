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

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.TopologySummary;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetricsUtilsTest {

  @Test
  public void testAddLatency() {
    Map<String, List<Double>> stats = Maps.newHashMap();
    String id = "spout";
    double lat = 0.01;
    MetricsUtils.addLatency(stats, id, lat);
    assertThat(stats).containsKey(id);
    assertThat(stats.get(id)).isNotNull().contains(lat);
  }

  @Test
  public void testGetTopologySummary() {
    ClusterSummary cs = mock(ClusterSummary.class);
    TopologySummary ts = mock(TopologySummary.class);
    String tsName = "benchmarks";
    String fakeName = "fake";

    when(cs.get_topologies()).thenReturn(Lists.newArrayList(ts));
    when(ts.get_name()).thenReturn(tsName);

    assertThat(MetricsUtils.getTopologySummary(cs, tsName)).isEqualTo(ts);
    assertThat(MetricsUtils.getTopologySummary(cs, fakeName)).isNull();
  }

}
