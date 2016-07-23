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

package storm.benchmark.tools;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;
import storm.benchmark.api.IBenchmark;
import storm.benchmark.metrics.IMetricsCollector;
import storm.benchmark.metrics.MetricsCollectorConfig;
import storm.benchmark.util.BenchmarkUtils;

public class LocalRunner {
  private static final Logger LOG = Logger.getLogger(Runner.class);
  private static final String PACKAGE = "storm.benchmark.benchmarks";

  public static void main(String[] args) throws Exception {
    if (null == args || args.length < 1) {
      throw new IllegalArgumentException("no benchmark is set");
    }
    run(args[0]);
  }

  private static void run(String name)
          throws ClassNotFoundException, IllegalAccessException,
          InstantiationException, AlreadyAliveException, InvalidTopologyException {
    LOG.info("running benchmark " + name);
    IBenchmark benchmark =  (IBenchmark) Runner.getApplicationFromName(PACKAGE + "." + name);
    Config config = new Config();
    config.putAll(Utils.readStormConfig());
    config.setDebug(true);
    StormTopology topology = benchmark.getTopology(config);
    LocalCluster localCluster = new LocalCluster();
    localCluster.submitTopology(name, config, topology);
    final int runtime = BenchmarkUtils.getInt(config, MetricsCollectorConfig.METRICS_TOTAL_TIME,
            MetricsCollectorConfig.DEFAULT_TOTAL_TIME);
    IMetricsCollector collector = benchmark.getMetricsCollector(config, topology);
    collector.run();
    try {
      Thread.sleep(runtime);
    } catch (InterruptedException e) {
      LOG.error("benchmark interrupted", e);
    }
    localCluster.shutdown();
  }
}
