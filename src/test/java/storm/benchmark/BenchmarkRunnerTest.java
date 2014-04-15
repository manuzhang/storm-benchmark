package storm.benchmark.topology;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;
import storm.benchmark.BenchmarkRunner;
import storm.benchmark.IBenchmark;

import static org.junit.Assert.assertTrue;

@RunWith(JUnitParamsRunner.class)
public class BenchmarkRunnerTest {

  @Test
  @Parameters(method = "getValidNames")
  public void returnBenchmarkFromValidName(String validName) throws Exception {
    assertTrue(BenchmarkRunner.getBenchmarkFrom(validName) instanceof IBenchmark);
  }

  @Test(expected = ClassNotFoundException.class)
  @Parameters(method = "getInValidNames")
  public void throwsExceptionFromInvalidName(String invalidName)
          throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    BenchmarkRunner.getBenchmarkFrom(invalidName);
  }


  private Object[] getValidNames() {
    return new String[][]{
            {"WordCount"}, {"SOL"},
            {"storm.benchmark.topology.WordCount"}, {"storm.benchmark.topology.SOL"}
    };
  }

  private Object[] getInValidNames() {
    return new String[][]{{"foo"}, {"bar"}};
  }
}
