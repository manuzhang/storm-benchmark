package storm.benchmark;

import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;

import java.util.concurrent.Semaphore;

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
    String packName = "storm.benchmark.topology";
    if (name.startsWith(packName)) {
      return getBenchmarkFrom(Class.forName(name));
    } else {
      return getBenchmarkFrom(Class.forName(packName + "." + name));
    }
  }
}
