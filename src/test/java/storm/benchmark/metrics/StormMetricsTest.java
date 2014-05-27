package storm.benchmark.metrics;

import backtype.storm.Config;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import storm.benchmark.BenchmarkConfig;

import java.io.PrintWriter;

import static org.mockito.Mockito.*;

public class StormMetricsTest {

  private static final String ANY_TOPOLOGY_NAME = "any_topology";
  private static final Config ANY_STORM_CONFIG = new Config();

  private static final StormMetrics metrics = new StormMetrics();

  @Test
  public void setConfigShouldAssignValuesToAllVariables() {
    BenchmarkConfig benchConfig = mock(BenchmarkConfig.class);
    when(benchConfig.getStormConfig()).thenReturn(ANY_STORM_CONFIG);
    when(benchConfig.getTopologyName()).thenReturn(ANY_TOPOLOGY_NAME);

    metrics.setConfig(benchConfig);


  }

  @DataProvider
  private Object[][] getDefaultValues() {
    return new Object[][] {
            { metrics.config, ANY_STORM_CONFIG },
            { metrics.topoName, ANY_TOPOLOGY_NAME },
            { metrics.pollInterval, StormMetrics.DEFAULT_POLL_INTERVAL },
            { metrics.totalTime, StormMetrics.DEFAULT_TOTAL_TIME },
            { metrics.msgSize, StormMetrics.DEFAULT_MESSAGE_SIZE },
            { metrics.path, StormMetrics.DEFAULT_PATH }
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
