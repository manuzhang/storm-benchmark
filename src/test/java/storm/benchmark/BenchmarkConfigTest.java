package storm.benchmark;

import backtype.storm.Config;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class BenchmarkConfigTest {

  @Test(dataProvider = "getDefaultConfigs")
  public void configsShouldBeSetToDefaultIfNotGiven (String name, Object defval) {
    BenchmarkConfig benchConfig = new BenchmarkConfig();
    Config stormConfig = benchConfig.getStormConfig();
    assertThat(stormConfig.get(name)).isEqualTo(defval);
  }

  @DataProvider
  private Object[][] getDefaultConfigs() {
    return new Object[][]{
            { Config.TOPOLOGY_NAME, BenchmarkConfig.DEFAULT_TOPOLOGY_NAME },
            { Config.TOPOLOGY_DEBUG, BenchmarkConfig.DISABLE_DEBUG },
            { Config.TOPOLOGY_WORKERS, BenchmarkConfig.DEFAULT_WORKERS },
            { Config.TOPOLOGY_ACKER_EXECUTORS, BenchmarkConfig.DEFAULT_ACKERS },
            { Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, BenchmarkConfig.DEFAULT_ERBSIZE },
            { Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, BenchmarkConfig.DEFAULT_ESBSIZE },
            { Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, BenchmarkConfig.DEFAULT_WRBSIZE },
            { Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, BenchmarkConfig.DEFAULT_WTBSIZE }
    };
  }
}
