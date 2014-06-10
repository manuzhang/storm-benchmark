/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package storm.benchmark.tools;


import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import storm.benchmark.api.IApplication;

import static org.junit.Assert.assertTrue;


public class RunnerTest {

  @Test(dataProvider = "getValidNames")
  public void getBenchmarkFromValidName(String validName) throws Exception {
    assertTrue(Runner.getApplicationFromName(validName) instanceof IApplication);
  }

  @Test(dataProvider = "getInValidNames", expectedExceptions = ClassNotFoundException.class)
  public void throwsExceptionFromInvalidName(String invalidName)
          throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    Runner.getApplicationFromName(invalidName);
  }

  @DataProvider
  private Object[][] getValidNames() {
    return new String[][]{
            {"storm.benchmark.benchmarks.FileReadWordCount"},
            {"storm.benchmark.benchmarks.SOL"},
            {"storm.benchmark.benchmarks.Grep"},
            {"storm.benchmark.benchmarks.PageViewCount"},
            {"storm.benchmark.benchmarks.UniqueVisitor"},
            {"storm.benchmark.benchmarks.KafkaWordCount"},
            {"storm.benchmark.benchmarks.DRPC"},
            {"storm.benchmark.benchmarks.RollingCount"},
            {"storm.benchmark.benchmarks.SOL"},
            {"storm.benchmark.benchmarks.TridentWordCount"},
            {"storm.benchmark.tools.producer.kafka.FileReadKafkaProducer"},
            {"storm.benchmark.tools.producer.kafka.PageViewKafkaProducer"}
    };
  }

  @DataProvider
  private Object[][] getInValidNames() {
    return new String[][]{{"foo"}, {"bar"}};
  }
}
