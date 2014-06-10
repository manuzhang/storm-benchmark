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

package storm.benchmark.benchmarks.common;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import storm.benchmark.benchmarks.*;
import storm.benchmark.metrics.BasicMetricsCollector;
import storm.benchmark.metrics.DRPCMetricsCollector;
import storm.benchmark.metrics.IMetricsCollector;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class StormBenchmarkTest {
  private static final Config config = new Config();
  private static final StormTopology topology = mock(StormTopology.class);

  @Test(dataProvider = "getStormBenchmark")
  public void getMetricsCollectorShouldReturnProperCollector(
          StormBenchmark benchmark, Class collectorClass) {
    IMetricsCollector collector = benchmark.getMetricsCollector(config, topology);
    assertThat(collector)
            .isNotNull()
            .isInstanceOf(collectorClass);
  }

  @DataProvider
  private Object[][] getStormBenchmark() {
    return new Object[][] {
            { new FileReadWordCount(), BasicMetricsCollector.class },
            { new SOL(), BasicMetricsCollector.class },
            { new RollingCount(), BasicMetricsCollector.class },
            { new Grep(), BasicMetricsCollector.class },
            { new DRPC(), DRPCMetricsCollector.class },
            { new TridentWordCount(), BasicMetricsCollector.class },
            { new PageViewCount(), BasicMetricsCollector.class },
            { new UniqueVisitor(), BasicMetricsCollector.class },
            { new DataClean(), BasicMetricsCollector.class }

    };
  }
}
