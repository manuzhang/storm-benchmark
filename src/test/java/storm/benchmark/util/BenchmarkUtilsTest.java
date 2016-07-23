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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.storm.Config;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;

public class BenchmarkUtilsTest {

  @Test(dataProvider = "getAvgs")
  public void testAvg(Iterable<Double> numbers, double expected) {
    assertThat(BenchmarkUtils.avg(numbers)).isEqualTo(expected);
  }

  @DataProvider
  private Object[][] getAvgs() {
    return new Object[][] {
            { Lists.newArrayList(1.0, 3.0, 8.0), 4.0 },
            { Sets.newHashSet(2.5, 4.5, 9.5), 5.5 }
    };
  }

  @Test(dataProvider = "getMaxes")
  public void testMax(Iterable<Double> numbers, double expected) {
    assertThat(BenchmarkUtils.max(numbers)).isEqualTo(expected);
  }

  @DataProvider
  private Object[][] getMaxes() {
    return new Object[][] {
            { Lists.newArrayList(1.0, 3.0, 3.0), 3.0 },
            { Sets.newHashSet(2.5, 4.5, 6.5), 6.5 }
    };
  }

  @Test
  public void testPutIfAbsent() {
    Map<String, Integer> map = Maps.newHashMap();
    BenchmarkUtils.putIfAbsent(map, "key1", 1);
    assertThat(map.get("key1")).isEqualTo(1);
    BenchmarkUtils.putIfAbsent(map, "key1", 2);
    assertThat(map.get("key1")).isEqualTo(1);
    BenchmarkUtils.putIfAbsent(map, "key2", 3);
    assertThat(map.get("key2")).isEqualTo(3);
  }

  @Test
  public void testGetInt() {
    Map map = Maps.newHashMap();
    assertThat(BenchmarkUtils.getInt(map, "key1", 1)).isEqualTo(1);
    map.put("key1", 2);
    assertThat(BenchmarkUtils.getInt(map, "key1", 1)).isEqualTo(2);
    map.put("key2", 3L);
    assertThat(BenchmarkUtils.getInt(map, "key2", 1)).isEqualTo(3);
  }

  @Test(dataProvider = "getNumberOfAckers")
  public void ifAckEnabledShouldReturnTrueForOneOrMoreAckers(int ackers, boolean expected) {
    Config config = new Config();

    assertThat(BenchmarkUtils.ifAckEnabled(config)).isFalse();
    config.put(Config.TOPOLOGY_ACKER_EXECUTORS, ackers);
    assertThat(BenchmarkUtils.ifAckEnabled(config)).isEqualTo(expected);
  }

  @DataProvider
  private Object[][] getNumberOfAckers() {
    return new Object[][] {
            { 0, false },
            { 1, true },
            { 2, true }
    };
  }
}
