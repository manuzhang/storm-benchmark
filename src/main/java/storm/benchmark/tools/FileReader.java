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

import storm.benchmark.util.FileUtils;

import java.io.Serializable;
import java.util.List;

public class FileReader implements Serializable {

  private static final long serialVersionUID = -7012334600647556267L;

  public final String file;
  private final List<String> contents;
  private int index = 0;
	private int limit = 0;

  public FileReader(String file) {
    this.file = file;
    if (this.file != null) {
      this.contents = FileUtils.readLines(this.getClass().getResourceAsStream(this.file));
      this.limit = contents.size();
    } else {
      throw new IllegalArgumentException("file name cannot be null");
    }
  }

  public String nextLine() {
    if (index >= limit) {
	    index = 0;
	  }
    String line = contents.get(index);
    index++;
    return line;
  }


}
