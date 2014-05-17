/*package storm.benchmark.metrics;


import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ganglia.GangliaReporter;
import info.ganglia.gmetric4j.gmetric.GMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;*/

/*
 * Listens for all metrics, reports to Ganglia
 *
 * To use, add this to your topology's configuration:
 *   conf.registerMetricsConsumer(storm.benchmark.metrics.GangliaMetricsConsumer, 1);
 *
 * Or edit the storm.yaml config file:
 *
 *   topology.metrics.consumer.register:
 *     - class: "storm.benchmark.metrics.GangliaMetricsConsumer"
 *       parallelism.hint: 1
 *
 */

/*public class GangliaMetricsConsumer implements IMetricsConsumer {
  private final Logger LOG = LoggerFactory.getLogger(GangliaMetricsConsumer.class);
  private final String GANGLIA_HOST = "192.168.1.71";

  @Override
  public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {}

  @Override
  public void cleanup() {}

  @Override
  public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
    try {
      final MetricRegistry registry = new MetricRegistry();
      final GMetric ganglia = new GMetric(GANGLIA_HOST, 8649, GMetric.UDPAddressingMode.MULTICAST, 1);
      final GangliaReporter reporter =
            GangliaReporter.forRegistry(registry)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build(ganglia);
      reporter.start(30, TimeUnit.SECONDS);
      for (final DataPoint p : dataPoints) {
        registry.register(MetricRegistry.name(p.name), new Gauge<Object>() {
          @Override
          public Object getValue() {
            return p.value;
          }
        });
      }
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
 }

}*/
