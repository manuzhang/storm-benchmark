package storm.benchmark.metrics;


/**
 * DRPCMetrics is meant to collect end-to-end latency for DRPC benchmarks
 * the latency is the time between DRPCClient submitting a query and
 * receiving the result
 */

import backtype.storm.utils.DRPCClient;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import storm.benchmark.util.FileUtils;

import java.io.PrintWriter;
import java.util.List;

public class DRPCMetrics extends StormMetrics {
  private static final Logger LOG = Logger.getLogger(DRPCMetrics.class);

  private final String function;
  private final List<String> args;
  private final String server;
  private final int port;
  int index = 0;

  public DRPCMetrics(String function, List<String> args, String server, int port) {
    this.function = function;
    this.args = args;
    this.server = server;
    this.port = port;
  }

  @Override
  public StormMetrics start() {
    long now = System.currentTimeMillis();

    final long endTime = now + totalTime;
    long totalLat = 0L;
    int count = 0;
    try {
      final String confFile = String.format(METRICS_CONF_FORMAT, path, topoName, now);
      final String dataFile = String.format(METRICS_FILE_FORMAT, path, topoName, now);
      PrintWriter confWriter = FileUtils.createFileWriter(confFile);
      PrintWriter dataWriter = FileUtils.createFileWriter(dataFile);
      writeConf(confWriter);
      while (now < endTime) {
        Thread.sleep(pollInterval);
        long lat = execute(nextArg());
        dataWriter.println(String.format("latency = %d", lat));
        dataWriter.flush();
        totalLat += lat;
        count++;
        now = System.currentTimeMillis();
      }
      long avgLat = 0 == count ? 0L : totalLat / count;
      dataWriter.println(String.format("average latency = %d", avgLat));
      dataWriter.close();
   } catch (Exception e) {
      LOG.error("fail to execute drpc function", e);
    }
    return this;
  }

  private void writeConf(PrintWriter writer) {
    writer.println(String.format("drpc.function = %s", function));
    writer.println(String.format("drpc.args = {%s}", Utils.join(args, ",")));
    writer.println(String.format("drpc.server = %s", server));
    writer.println(String.format("drpc.port = %s", port));
    writer.close();
  }

  private String nextArg() {
    if (args.size() == index) {
      index = 0;
    }
    String ret = args.get(index);
    index++;
    return ret;
  }

  private long execute(String arg) throws Exception {
    DRPCClient client = new DRPCClient(server, port);
    long start = System.currentTimeMillis();
    String result = client.execute(function, arg);
    long end = System.currentTimeMillis();
    LOG.info(String.format("%s(\"%s\") = %s", function, arg, result));
    return end - start;
  }
}
