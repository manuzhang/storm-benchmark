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

package storm.benchmark.lib.spout.pageview;

import org.testng.annotations.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class PageViewTest {

  @Test
  public void testFromString() {
    String pvString = "http://foo.com\t200\t100000\t1";
    PageView pageView = PageView.fromString(pvString);
    assertThat(pageView.url).isEqualTo("http://foo.com");
    assertThat(pageView.status).isEqualTo(200);
    assertThat(pageView.zipCode).isEqualTo(100000);
    assertThat(pageView.userID).isEqualTo(1);
    assertThat(pageView.toString()).isEqualTo(pvString);
  }
}
