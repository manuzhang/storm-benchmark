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

package storm.benchmark.util;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.PrintWriter;

import static org.fest.assertions.api.Assertions.assertThat;

public class FileUtilsTest {

  private static final String PARENT = "./test/";
  private static final String PATH = PARENT + "file.txt";
  private static File dir;
  private static File file;
  private static PrintWriter writer;

  @BeforeMethod
  public void setUp() {
    dir = new File(PARENT);
    file = new File(PATH);
    assertThat(dir).doesNotExist();
    assertThat(file).doesNotExist();
  }

  @AfterMethod
  public void cleanUp() {
    writer.close();
    file.delete();
    assertThat(file).doesNotExist();
    dir.delete();
    assertThat(dir).doesNotExist();
  }

  @Test
  public void testFileCreateWhenParentDoesNotExist() {
    writer = FileUtils.createFileWriter(PARENT, PATH);
    assertThat(writer).isNotNull();
    assertThat(dir).exists();
    assertThat(file).exists();
  }

  @Test
  public void testFileCreateWhenParentExists() {
    dir.mkdirs();
    assertThat(dir).exists();
    writer = FileUtils.createFileWriter(PARENT, PATH);
    assertThat(writer).isNotNull();
    assertThat(file).exists();
  }

}
