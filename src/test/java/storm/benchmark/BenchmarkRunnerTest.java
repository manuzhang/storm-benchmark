package storm.benchmark;


import org.mockito.InOrder;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;


public class BenchmarkRunnerTest {

  @Test
  public void verifyInvocationInOrder() throws Exception {
    BenchmarkRunner runner = new BenchmarkRunner();
    IBenchmark benchmark = mock(IBenchmark.class);
    when(benchmark.parseOptions(anyMap())).thenReturn(benchmark);
    when(benchmark.buildTopology()).thenReturn(benchmark);
    when(benchmark.submit()).thenReturn(benchmark);
    when(benchmark.startMetrics()).thenReturn(benchmark);
    runner.run(benchmark);

    InOrder inOrder = Mockito.inOrder(benchmark);
    inOrder.verify(benchmark, times(1)).parseOptions(anyMap());
    inOrder.verify(benchmark, times(1)).buildTopology();
    inOrder.verify(benchmark, times(1)).submit();
    inOrder.verify(benchmark, times(1)).startMetrics();
  }


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
            {"RollingSort"}, {"storm.benchmark.topology.RollingSort"},
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
