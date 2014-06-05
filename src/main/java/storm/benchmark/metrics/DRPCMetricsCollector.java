package storm.benchmark.metrics;


/**
 * DRPCMetricsCollector is meant to collect end-to-end latency for DRPC benchmarks
 * the latency is the time between DRPCClient submitting a query and
 * receiving the result
 */

import backtype.storm.Config;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.DRPCClient;
import org.apache.log4j.Logger;
import org.apache.thrift7.TException;
import storm.benchmark.util.FileUtils;

import java.io.PrintWriter;
import java.util.List;

public class DRPCMetricsCollector extends BasicMetricsCollector {
  private static final Logger LOG = Logger.getLogger(DRPCMetricsCollector.class);

  private final String function;
  private final List<String> args;
  private final String server;
  private final int port;
  int index = 0;

  public DRPCMetricsCollector(Config config, StormTopology topology,
                              String function, List<String> args, String server, int port) {
    super(config, topology);
    this.function = function;
    this.args = args;
    this.server = server;
    this.port = port;
  }

  @Override
  public void collect() {
    long now = System.currentTimeMillis();

    final long endTime = now + totalTime;
    long totalLat = 0L;
    int count = 0;
    try {
      final String confFile = String.format(METRICS_CONF_FORMAT, path, topoName, now);
      final String dataFile = String.format(METRICS_FILE_FORMAT, path, topoName, now);
      PrintWriter confWriter = FileUtils.createFileWriter(path, confFile);
      PrintWriter dataWriter = FileUtils.createFileWriter(path, dataFile);
      writeStormConfig(confWriter);
      while (now < endTime) {
        Thread.sleep(pollInterval);
        long lat = execute(nextArg(), dataWriter);
        totalLat += lat;
        count++;
        now = System.currentTimeMillis();
      }
      double avgLat = 0 == count ? 0.0 : (double) totalLat / count;
      dataWriter.println(String.format("average latency = %f ms", avgLat));
      dataWriter.close();
    } catch (DRPCExecutionException e) {
      LOG.error("fail to execute drpc function", e);
    } catch (TException e) {
      LOG.error("thrift error", e);
    } catch (InterruptedException e) {
      LOG.error("interrupted", e);
    }
  }

  private String nextArg() {
    if (args.size() == index) {
      index = 0;
    }
    String ret = args.get(index);
    index++;
    return ret;
  }

  private long execute(String arg, PrintWriter writer) throws TException, DRPCExecutionException {
    LOG.debug(String.format("executing %s('%s')", function, arg));
    DRPCClient client = new DRPCClient(server, port);
    long start = System.currentTimeMillis();
    String result = client.execute(function, arg);
    long end = System.currentTimeMillis();
    long latency = end - start;
    writer.println(String.format("%s('%s') = %s, latency = %d ms", function, arg, result, latency));
    writer.flush();
    return latency;
  }
}
