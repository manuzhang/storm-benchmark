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
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.org.yaml.snakeyaml.Yaml;
import org.apache.storm.shade.org.yaml.snakeyaml.constructor.SafeConstructor;
import org.apache.storm.utils.Utils;
import storm.benchmark.api.IApplication;
import storm.benchmark.api.IBenchmark;
import storm.benchmark.api.IProducer;
import storm.benchmark.metrics.IMetricsCollector;
import storm.benchmark.metrics.MetricsCollectorConfig;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

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

    config.putAll(Utils.readStormConfig());
    if (args.length > 1) {
      config.putAll(readConfig(args[1]));
    }

    run(args[0]);
  }

  public static void run(String name)
      throws ClassNotFoundException, IllegalAccessException,
      InstantiationException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {
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
      ClassNotFoundException, IllegalAccessException, InstantiationException, AuthorizationException {
    runApplication(benchmark);
    if (isMetricsEnabled()) {
      IMetricsCollector collector = benchmark.getMetricsCollector(config, topology);
      collector.run();
    }
  }

  public static void runProducer(IProducer producer)
      throws AlreadyAliveException, InvalidTopologyException,
      ClassNotFoundException, IllegalAccessException, InstantiationException, AuthorizationException {
    runApplication(producer);
  }


  public static IApplication getApplicationFromName(String name)
          throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    return (IApplication) Class.forName(name).newInstance();
  }

  private static void runApplication(IApplication app)
      throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
    String name = (String) config.get(Config.TOPOLOGY_NAME);
    topology = app.getTopology(config);
    StormSubmitter.submitTopology(name, config, topology);
  }

  private static boolean isMetricsEnabled() {
    return (Boolean) config.get(MetricsCollectorConfig.METRICS_ENABLED);
  }

  private static Map<String, Object> readConfig(String config) {
    Map<String, Object> ret = new HashMap<String, Object>();
    try {
      Yaml yaml = new Yaml(new SafeConstructor());
      InputStream input = new FileInputStream(config);
      try {
        ret = (Map<String, Object>) yaml.load(new InputStreamReader(input));
      } catch (Exception e) {
        LOG.error("failed to load config file " + config);
      } finally {
        input.close();
      }
    } catch (FileNotFoundException e) {
      LOG.error("failed to find config file " + config);
    } catch (Throwable t) {
      LOG.error(t.getMessage());
    }
    return ret;
  }
}
