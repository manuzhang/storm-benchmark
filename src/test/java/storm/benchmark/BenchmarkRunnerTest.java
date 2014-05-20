package storm.benchmark;


import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.junit.Assert.assertTrue;

public class BenchmarkRunnerTest {

  @Test(dataProvider = "getValidNames")
  public void returnBenchmarkFromValidName(String validName) throws Exception {
    assertTrue(BenchmarkRunner.getBenchmarkFrom(validName) instanceof IBenchmark);
  }

  @Test(dataProvider = "getInValidNames", expectedExceptions = ClassNotFoundException.class)
  public void throwsExceptionFromInvalidName(String invalidName)
          throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    BenchmarkRunner.getBenchmarkFrom(invalidName);
  }

  @DataProvider
  private Object[][] getValidNames() {
    return new String[][]{
            {"FileReadWordCount"}, {"storm.benchmark.topology.FileReadWordCount"},
            {"SOL"}, {"storm.benchmark.topology.SOL"},
            {"Grep"}, {"storm.benchmark.topology.Grep"},
            {"KafkaPageView"}, {"storm.benchmark.topology.KafkaPageView"},
            {"KafkaUniqueVisitor"}, {"storm.benchmark.topology.KafkaUniqueVisitor"},
            {"KafkaWordCount"}, {"storm.benchmark.topology.KafkaWordCount"},
            {"DRPC"}, {"storm.benchmark.topology.DRPC"},
            {"RollingSort"}, {"storm.benchmark.topology.RollingSort"},
            {"SOL"}, {"storm.benchmark.topology.SOL"},
            {"TridentWordCount"}, {"storm.benchmark.topology.TridentWordCount"},
            {"storm.benchmark.kafka.KafkaFileProducer"},
            {"storm.benchmark.kafka.KafkaPageViewProducer"}
    };
  }

  @DataProvider
  private Object[][] getInValidNames() {
    return new String[][]{{"foo"}, {"bar"}};
  }
}
