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

package storm.benchmark.api;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import storm.benchmark.metrics.IMetricsCollector;

/**
 * users could write their own benchmarks against this interface.
 * a benchmark is a storm application augmented with a
 * {@link storm.benchmark.metrics.IMetricsCollector}, which collects
 * metrics like throughout or latency
 */
public interface IBenchmark extends IApplication {
  public IMetricsCollector getMetricsCollector(Config config, StormTopology topology);
}
