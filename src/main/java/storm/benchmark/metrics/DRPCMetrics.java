package storm.benchmark.metrics;


/**
 * DRPCMetrics is meant to collect end-to-end latency for DRPC benchmarks
 * the latency is the time between DRPCClient submitting a query and
 * receiving the result
 */

import backtype.storm.utils.DRPCClient;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;

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
    writeConf();
  }

  @Override
  public StormMetrics start() {
    long now = System.currentTimeMillis();
    final long endTime = now + total;
    long totalLat = 0L;
    int count = 0;
    try {
      while (now < endTime) {
        Thread.sleep(poll);
        long lat = execute(nextArg());
        fileWriter.println(String.format("latency = %d", lat));
        fileWriter.flush();
        totalLat += lat;
        count++;
        now = System.currentTimeMillis();
      }
      long avgLat = 0 == count ? 0L : totalLat / count;
      fileWriter.write(String.format("average latency = %d\n", avgLat));
      fileWriter.close();
   } catch (Exception e) {
      LOG.error("fail to execute drpc function", e);
    }
    return this;
  }

  private void writeConf() {
    confWriter.println(String.format("drpc.function = %s", function));
    confWriter.println(String.format("drpc.args = {%s}", Utils.join(args, ",")));
    confWriter.println(String.format("drpc.server = %s", server));
    confWriter.println(String.format("drpc.port = %s", port));
    confWriter.close();
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
