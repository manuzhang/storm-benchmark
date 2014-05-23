package storm.benchmark.metrics;

import backtype.storm.generated.*;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import storm.benchmark.util.MetricsUtils;
import storm.benchmark.util.BenchmarkUtils;

import java.io.PrintWriter;
import java.util.*;

public class BasicMetrics extends StormMetrics {

  private static final Logger LOG = Logger.getLogger(BasicMetrics.class);

  static final String SPOUT_AVG_LATENCY_FORMAT = "%.1f";
  static final String SPOUT_MAX_LATENCY_FORMAT = "%.1f";

  private List<String> header = new LinkedList<String>();

  @Override
  public IMetrics start() {
    long now = System.currentTimeMillis();
    long endTime = now + total;
    MetricsState state = new MetricsState();
    state.startTime = now;
    state.lastTime = now;

    writeOutStormConfig(confWriter);
    writeHeader(fileWriter);
    Nimbus.Client client = getNimbusClient();

    try {
      boolean live = true;
      while (live && now < endTime) {
        live = pollNimbus(client, now, state, fileWriter);
        Utils.sleep(poll);
        now = System.currentTimeMillis();
      }
    } catch (Exception e) {
      LOG.error("BasicMetrics failed! ", e);
    } finally {
      fileWriter.close();
      confWriter.close();
    }
    return this;
  }


  boolean pollNimbus(Nimbus.Client client, long now, MetricsState state, PrintWriter writer)
    throws Exception {
    ClusterSummary cs = client.getClusterInfo();
    if (null == cs) {
      return false;
    }
    TopologySummary ts = MetricsUtils.getTopologySummary(cs, topoName);
    if (null == ts) {
      return false;
    }

    long timeTotal = now - state.startTime;
    int numWorkers = ts.get_num_workers();
    int numExecutors = ts.get_num_executors();
    int numTasks = ts.get_num_tasks();
    metrics.put(TIME, String.format(TIME_FORMAT, timeTotal / 1000));
    metrics.put(WORKERS, Integer.toString(numWorkers));
    metrics.put(EXECUTORS, Integer.toString(numExecutors));
    metrics.put(TASKS, Integer.toString(numTasks));

    int totalSlots = 0;
    int usedSlots = 0;
    for (SupervisorSummary ss : cs.get_supervisors()) {
      totalSlots += ss.get_num_workers();
      usedSlots += ss.get_num_used_workers();
    }
    metrics.put(TOTAl_SLOTS, Integer.toString(totalSlots));
    metrics.put(USED_SLOTS, Integer.toString(usedSlots));

    long overallTransferred= 0;
    long spoutTransferred = 0;
    long spoutAcked = 0;
    int executorsWithMetircs = 0;
    int spoutExecutors = 0;
    Map<String, List<Double>> comLat = new HashMap<String, List<Double>>();
    TopologyInfo info = client.getTopologyInfo(ts.get_id());
    for (ExecutorSummary es : info.get_executors()) {
      String id = es.get_component_id();
      if (Utils.isSystemId(id)) {
        continue;
      }
      ExecutorStats exeStats = es.get_stats();
      if (exeStats != null) {
        executorsWithMetircs++;
        long transferred = MetricsUtils.getTransferred(exeStats, ALL_TIME, Utils.DEFAULT_STREAM_ID);
        overallTransferred += transferred;
        ExecutorSpecificStats specs = exeStats.get_specific();
        if (specs != null && specs.is_set_spout()) {
          spoutExecutors++;
          spoutTransferred += transferred;
          SpoutStats spStats = specs.get_spout();
          spoutAcked += MetricsUtils.getSpoutAcked(spStats, ALL_TIME, Utils.DEFAULT_STREAM_ID);
          double lat = MetricsUtils.getSpoutCompleteLatency(spStats, ALL_TIME, Utils.DEFAULT_STREAM_ID);
          MetricsUtils.addLatency(comLat, id, lat);
        }
      }
    }
    metrics.put(EXECUTORS_METRICS, Integer.toString(executorsWithMetircs));
    metrics.put(SPOUT_EXECUTORS, Integer.toString(spoutExecutors));

    long timeDiff = now - state.lastTime;
    long overallDiff = overallTransferred - state.overallTransferred;
    long spoutDiff = spoutTransferred - state.spoutTransferred;
    long throughput = (long) MetricsUtils.getThroughput(overallDiff, timeDiff);
    double throughputMB = (long) MetricsUtils.getThroughputMB(overallDiff, timeDiff, msgSize);
    long spoutThroughput = (long) MetricsUtils.getThroughput(spoutDiff, timeDiff);
    double spoutThroughputMB = (long) MetricsUtils.getThroughputMB(spoutDiff, timeDiff, msgSize);
    metrics.put(TRANSFERRED, Long.toString(overallDiff));
    metrics.put(THROUGHPUT, Long.toString(throughput));
    metrics.put(THROUGHPUT_MB, String.format(THROUGHPUT_MB_FORMAT, throughputMB));
    metrics.put(SPOUT_TRANSFERRED, Long.toString(spoutDiff));
    metrics.put(SPOUT_ACKED, Long.toString(spoutAcked));
    metrics.put(SPOUT_THROUGHPUT, Long.toString(spoutThroughput));
    metrics.put(SPOUT_THROUGHPUT_MB,
            String.format(SPOUT_THROUGHPUT_MB_FORMAT, spoutThroughputMB));

    for (String id : spouts.keySet()) {
      List<Double> lats = comLat.get(id);
      double avg = null == lats ? 0.0 : BenchmarkUtils.avg(lats);
      double max = null == lats ? 0.0 : BenchmarkUtils.max(lats);
      metrics.put(MetricsUtils.getSpoutMaxCompleteLatencyTitle(id),
              String.format(SPOUT_AVG_LATENCY_FORMAT, avg));
      metrics.put(MetricsUtils.getSpoutAvgCompleteLatencyTitle(id),
              String.format(SPOUT_MAX_LATENCY_FORMAT, max));
    }

    writeLine(writer);

    state.lastTime = now;
    state.overallTransferred = overallTransferred;
    state.spoutTransferred = spoutTransferred;
    return true;
  }

  void writeHeader(PrintWriter writer) {
    header.add(TIME);
    header.add(TOTAl_SLOTS);
    header.add(USED_SLOTS);
    header.add(WORKERS);
    header.add(TASKS);
    header.add(EXECUTORS);
    header.add(EXECUTORS_METRICS);
    header.add(TRANSFERRED);
    header.add(THROUGHPUT);
    header.add(THROUGHPUT_MB);
    header.add(SPOUT_EXECUTORS);
    header.add(SPOUT_TRANSFERRED);
    header.add(SPOUT_ACKED);
    header.add(SPOUT_THROUGHPUT);
    header.add(SPOUT_THROUGHPUT_MB);
    for (String id : spouts.keySet()) {
      header.add(MetricsUtils.getSpoutAvgCompleteLatencyTitle(id));
      header.add(MetricsUtils.getSpoutMaxCompleteLatencyTitle(id));
    }
    writer.println(Utils.join(header, ","));
    writer.flush();
  }

  void writeLine(PrintWriter writer) {
    List<String> line = new LinkedList<String>();
    for (String h : header) {
      line.add(metrics.get(h));
    }
    writer.println(Utils.join(line, ","));
    writer.flush();
  }


}
