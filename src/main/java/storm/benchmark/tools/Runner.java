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

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import storm.benchmark.api.IApplication;
import storm.benchmark.api.IBenchmark;
import storm.benchmark.api.IProducer;
import storm.benchmark.metrics.IMetricsCollector;
import storm.benchmark.metrics.MetricsCollectorConfig;

/**
 * Runner is the main class of storm benchmark
 * It instantiates an IBenchmark from passed-in name and then run it
 */
public class Runner {
  private static final Logger LOG = Logger.getLogger(Runner.class);

  private static Config config = new Config();
  private static StormTopology topology;

  public static void main(String[] args) throws Exception {
    if (null == args || args.length < 1) {
      throw new IllegalArgumentException("no benchmark is set");
    }

    run(args[0]);
  }

  public static void run(String name)
          throws ClassNotFoundException, IllegalAccessException,
          InstantiationException, AlreadyAliveException, InvalidTopologyException {
    if (name.startsWith("storm.benchmark.benchmarks")) {
      LOG.info("running benchmark " + name);
      runBenchmark((IBenchmark) getApplicationFromName(name));
    } else if (name.startsWith("storm.benchmark.tools.producer")) {
      LOG.info("running producer " + name);
      runProducer((IProducer) getApplicationFromName(name));
    } else {
      throw new RuntimeException(name + " is neither benchmark nor producer");
    }
  }

  public static void runBenchmark(IBenchmark benchmark)
          throws AlreadyAliveException, InvalidTopologyException,
          ClassNotFoundException, IllegalAccessException, InstantiationException {
    runApplication(benchmark);
    if (isMetricsEnabled()) {
      IMetricsCollector collector = benchmark.getMetricsCollector(config, topology);
      collector.run();
    }
  }

  public static void runProducer(IProducer producer)
          throws AlreadyAliveException, InvalidTopologyException,
          ClassNotFoundException, IllegalAccessException, InstantiationException {
    runApplication(producer);
  }


  public static IApplication getApplicationFromName(String name)
          throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    return (IApplication) Class.forName(name).newInstance();
  }

  private static void runApplication(IApplication app)
          throws AlreadyAliveException, InvalidTopologyException {
    config.putAll(Utils.readStormConfig());
    String name = (String) config.get(Config.TOPOLOGY_NAME);
    topology = app.getTopology(config);
    StormSubmitter.submitTopology(name, config, topology);
  }

  private static boolean isMetricsEnabled() {
    return (Boolean) config.get(MetricsCollectorConfig.METRICS_ENABLED);
  }
}
