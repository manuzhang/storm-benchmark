package storm.benchmark;

import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;

/**
 * BenchmarkRunner is the main class for StormBenchmark
 * It instantiates a benchmark from passed-in name (either with or with out package name)
 */
public class BenchmarkRunner {
  private static final Logger LOG = Logger.getLogger(BenchmarkRunner.class);

  public static void main(String[] args) throws Exception {
    if (null == args || args.length < 1) {
      throw new IllegalArgumentException("no benchmark is set");
    }

    IBenchmark benchmark  = getBenchmarkFrom(args[0]);
    LOG.info("starting benchmark " + args[0]);
    run(benchmark);
  }

  public static void run(IBenchmark benchmark) throws Exception {
    benchmark.parseOptions(Utils.readCommandLineOpts())
            .buildTopology()
            .submit()
            .startMetrics();
  }

  public static IBenchmark getBenchmarkFrom(Class clazz)
          throws IllegalAccessException, InstantiationException {
    return (IBenchmark) clazz.newInstance();
  }

  public static IBenchmark getBenchmarkFrom(String name)
          throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    String topologyPkg = "storm.benchmark.topology";
    String kafkaPkg = "storm.benchmark.kafka";
    if (name.startsWith(topologyPkg) || name.startsWith(kafkaPkg)) {
      return getBenchmarkFrom(Class.forName(name));
    } else if (name.endsWith("Producer")) {
      return getBenchmarkFrom(Class.forName(kafkaPkg + "." + name));
    } else {
      return getBenchmarkFrom(Class.forName(topologyPkg + "." + name));
    }
  }
}
