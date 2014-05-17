package storm.benchmark.metrics;

import backtype.storm.generated.*;
import org.apache.log4j.Logger;
import storm.benchmark.util.Util;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BasicMetrics extends StormMetrics {

  private static final Logger LOG = Logger.getLogger(BasicMetrics.class);

  private static final String SPOUT_AVG_LATENCY_FORMAT = "%.1f";
  private static final String SPOUT_MAX_LATENCY_FORMAT = "%.1f";
  private static final String STREAM = "default";

  private List<String> header = new LinkedList<String>();

  @Override
  public IMetrics start() {
    long now = System.currentTimeMillis();
    long endTime = now + total;
    MetricsState state = new MetricsState();
    state.startTime = now;
    state.lastTime = now;

    writeOutCommandLineOpts(confWriter);
    writeHeader(fileWriter);
    Nimbus.Client client = getNimbusClient();

    try {
      boolean live = true;
      while (live && now < endTime) {
        live = pollNimbus(client, now, state, fileWriter);
        Thread.sleep(poll);
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


  private boolean pollNimbus(Nimbus.Client client, long now, MetricsState state, PrintWriter writer)
    throws Exception {
    final String name = config.getTopologyName();
    ClusterSummary cs = client.getClusterInfo();
    if (null == cs) {
      return false;
    }
    TopologySummary ts = getTopologySummary(cs, name);
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
      // filter out acker and metrics stats
      if (id.startsWith("__acker") || id.startsWith("_metrics")) {
        continue;
      }
      ExecutorStats exeStats = es.get_stats();
      if (exeStats != null) {
        executorsWithMetircs++;
        long transferred = getTransferred(exeStats);
        overallTransferred += transferred;
        ExecutorSpecificStats specs = exeStats.get_specific();
        if (specs != null && specs.is_set_spout()) {
          spoutExecutors++;
          spoutTransferred += transferred;
          SpoutStats spStats = specs.get_spout();
          double lat = getSpoutCompleteLatency(spStats, ALL_TIME, STREAM);
          spoutAcked += getSpoutAcked(spStats, ALL_TIME, STREAM);
          updateLatency(id, lat, comLat);
        }
      }
    }
    metrics.put(EXECUTORS_METRICS, Integer.toString(executorsWithMetircs));
    metrics.put(SPOUT_EXECUTORS, Integer.toString(spoutExecutors));

    long timeDiff = now - state.lastTime;
    long overallDiff = overallTransferred - state.overallTransferred;
    long spoutDiff = spoutTransferred - state.spoutTransferred;
    long throughput = (long) getThroughput(overallDiff, timeDiff);
    double throughputMB = (long) getThroughputMB(overallDiff, timeDiff);
    long spoutThroughput = (long) getThroughput(spoutDiff, timeDiff);
    double spoutThroughputMB = (long) getThroughputMB(spoutDiff, timeDiff);
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
      double avg = null == lats ? 0.0 : Util.avg(lats);
      double max = null == lats ? 0.0 : Util.max(lats);
      metrics.put(SPOUT_AVG_COMPELETE_LATENCY(id),
              String.format(SPOUT_AVG_LATENCY_FORMAT, avg));
      metrics.put(SPOUT_MAX_COMPELETE_LATENCY(id),
              String.format(SPOUT_MAX_LATENCY_FORMAT, max));
    }

    writeLine(writer);

    state.lastTime = now;
    state.overallTransferred = overallTransferred;
    state.spoutTransferred = spoutTransferred;
    return true;
  }

  private TopologySummary getTopologySummary(ClusterSummary cs, String name) {
    for (TopologySummary ts : cs.get_topologies()) {
      if (name.equals(ts.get_name())) {
        return ts;
      }
    }
    return null;
  }

  private double getSpoutCompleteLatency(SpoutStats stats, String window, String stream) {
    Map<String, Map<String, Double>> latAll = stats.get_complete_ms_avg();
    if (latAll != null) {
      // latency in a time window
      Map<String, Double> latWin = latAll.get(window);
      if (latWin != null) {
        // latency in a stream
        Double latStr = latWin.get(stream);
        if (latStr != null) {
          return latStr;
        }
      }
    }
    return 0.0;
  }

  private long getSpoutAcked(SpoutStats stats, String window, String stream) {
    Map<String, Map<String, Long>> ackedAll = stats.get_acked();
    if (ackedAll != null) {
      Map<String, Long> ackedWin = ackedAll.get(window);
      if (ackedWin != null) {
        Long ackedStr = ackedWin.get(stream);
        if (ackedStr != null) {
          return ackedStr;
        }
      }
    }
    return 0;
  }

  private long getTransferred(ExecutorStats stats) {
    Map<String, Map<String, Long>> transAll = stats.get_transferred();
    if (transAll != null) {
      Map<String, Long> transWin = transAll.get(ALL_TIME);
      if (transWin != null) {
        Long transStr = transWin.get(STREAM);
        if (transStr != null) {
          return transStr;
        }
      }
    }
    return 0;
  }

  // messages per second
  private double getThroughput(long throughputDiff, long timeDiff) {
    return (0 == timeDiff) ? 0.0 : throughputDiff / (timeDiff / 1000.0);
  }

  // MB per second
  private double getThroughputMB(long throughputDiff, long timeDiff) {
    return (0 == timeDiff) ? 0.0 : (throughputDiff * msgSize) / (timeDiff / 1000.0) / THROUGHPUT_UNIT;
  }

  private void updateLatency(String id, double lat, Map<String, List<Double>> stats) {
    if (stats.containsKey(id)) {
      stats.get(id).add(lat);
    } else {
      List<Double> list = new LinkedList<Double>();
      list.add(lat);
      stats.put(id, list);
    }
  }

  private void writeHeader(PrintWriter writer) {
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
      header.add(SPOUT_AVG_COMPELETE_LATENCY(id));
      header.add(SPOUT_MAX_COMPELETE_LATENCY(id));
    }
    writer.println(Util.join(header, ","));
    writer.flush();
  }

  private void writeLine(PrintWriter writer) {
    List<String> line = new LinkedList<String>();
    for (String h : header) {
      line.add(metrics.get(h));
    }
    writer.println(Util.join(line, ","));
    writer.flush();
  }

  private String SPOUT_AVG_COMPELETE_LATENCY(String id) {
    return id + "_avg_complete_latency(ms)";
  }

  private String SPOUT_MAX_COMPELETE_LATENCY(String id) {
    return id + "_max_complete_lantency(ms)";
  }

  private static class MetricsState {
    long overallTransferred = 0;
    long spoutTransferred = 0;
    long lastTime = 0;
    long startTime = 0;
  }
}
