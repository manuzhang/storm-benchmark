package storm.benchmark;


import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.junit.Assert.assertTrue;


public class BenchmarkRunnerTest {

  @Test(dataProvider = "getValidNames")
  public void getBenchmarkFromValidName(String validName) throws Exception {
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
            {"PageViewCount"}, {"storm.benchmark.topology.PageViewCount"},
            {"UniqueVisitor"}, {"storm.benchmark.topology.UniqueVisitor"},
            {"KafkaWordCount"}, {"storm.benchmark.topology.KafkaWordCount"},
            {"DRPC"}, {"storm.benchmark.topology.DRPC"},
            {"RollingCount"}, {"storm.benchmark.topology.RollingCount"},
            {"SOL"}, {"storm.benchmark.topology.SOL"},
            {"TridentWordCount"}, {"storm.benchmark.topology.TridentWordCount"},
            {"KafkaFileReadProducer"}, {"storm.benchmark.kafka.KafkaFileReadProducer"},
            {"KafkaPageViewProducer"}, {"storm.benchmark.kafka.KafkaPageViewProducer"}
    };
  }

  @DataProvider
  private Object[][] getInValidNames() {
    return new String[][]{{"foo"}, {"bar"}};
  }
}
