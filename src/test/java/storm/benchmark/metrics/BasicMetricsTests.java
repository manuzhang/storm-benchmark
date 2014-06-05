package storm.benchmark.metrics;

import backtype.storm.Config;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.PrintWriter;

import static org.mockito.Mockito.*;

public class BasicMetricsTests {

  private static final String ANY_TOPOLOGY_NAME = "any_topology";
  private static final Config ANY_STORM_CONFIG = new Config();

  private static final BasicMetricsCollector metrics = mock(BasicMetricsCollector.class);


  @DataProvider
  private Object[][] getDefaultValues() {
    return new Object[][] {
            { metrics.config, ANY_STORM_CONFIG },
            { metrics.topoName, ANY_TOPOLOGY_NAME },
            { metrics.pollInterval, BasicMetricsCollector.DEFAULT_POLL_INTERVAL },
            { metrics.totalTime, BasicMetricsCollector.DEFAULT_TOTAL_TIME },
            { metrics.msgSize, BasicMetricsCollector.DEFAULT_MESSAGE_SIZE },
            { metrics.path, BasicMetricsCollector.DEFAULT_PATH }
    };
  }


  @Test
  public void testWriteStormConfig() {
    PrintWriter writer = mock(PrintWriter.class);
    metrics.writeStormConfig(writer);

    verify(writer, times(metrics.config.size())).println(anyString());
    verify(writer, times(1)).flush();
  }




}
