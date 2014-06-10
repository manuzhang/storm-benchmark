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


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import storm.benchmark.lib.spout.pageview.PageView;

import java.io.Serializable;
import java.util.*;


/**
 *  forked from https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/streaming/examples/clickstream/PageViewGenerator.scala
 */

public class PageViewGenerator implements Serializable {

  private static final long serialVersionUID = 6825565414146438901L;
  public static final Distribution<String> PAGES = new Distribution<String>(new Pair("foo.com", 0.7),
                                                            new Pair("foo.news.com", 0.2),
                                                            new Pair("foo.contact.com", 0.1));
  public static final Distribution<Integer> HTTP_STATUS = new Distribution<Integer>(new Pair(200, 0.95), new Pair(404, 0.05));
  public static final Distribution<Integer> USER_ZIP_CODE = new Distribution<Integer>(new Pair(94709, 0.5), new Pair(94117, 0.5));
  public static final Distribution<Integer> USER_ID = Distribution.intEvenDistribution(0, 100);

  public String getNextClickEvent() {
    String page = pickFromDistribution(PAGES);
    int status = pickFromDistribution(HTTP_STATUS);
    int zip = pickFromDistribution(USER_ZIP_CODE);
    int id = pickFromDistribution(USER_ID);
    return new PageView(page, status, zip, id).toString();
  }

  public Map<Integer, List<Integer>> genFollowersDB() {
    Map<Integer, List<Integer>> db = Maps.newHashMap();
    for (int i = 0; i < 100; i++) {
      db.put(i, Lists.newArrayList(getFollowers(i)));
    }
    return db;
  }

  public Set<Integer> getFollowers(int id) {
    Random random = new Random(id);
    int num = random.nextInt(100);
    Set<Integer> followers = new HashSet<Integer>(num);
    for (int i = 0; i < num; i++) {
      followers.add(pickFromDistribution(USER_ID));
    }
    return followers;
  }

  private <T> T pickFromDistribution(Distribution<T> dist) {

    final double rand = new Random().nextDouble();
    double total = 0.0;
    for (T d : dist.getKeySet()) {
      total = total + dist.getProbability(d);
      if (total > rand) {
        return d;
      }
    }
    return dist.getKeySet().iterator().next();
  }

  public static class Distribution<T> implements Serializable {

    private static final long serialVersionUID = 115923825637621162L;
    private final Map<T, Double> dist = new HashMap<T, Double>();

    public Distribution(List<Pair<T, Double>> data) {
      for (Pair<T, Double> p : data) {
        dist.put(p.key, p.val);
      }
    }

    public Distribution(Pair<T, Double>... data) {
      for (Pair<T, Double> p : data) {
        dist.put(p.key, p.val);
      }
    }

    public static Distribution intEvenDistribution(int start, int end) {
      if (start >= end) {
        throw new IllegalArgumentException(String.format("invalid arguments [%d, %d) to generate even distribution", start, end));
      }
      List<Pair<Integer, Double>> pairs = new ArrayList<Pair<Integer, Double>>();
      for (int i = start; i < end; i++) {
        pairs.add(new Pair(i, 1.0 / (end - start)));
      }
      return new Distribution(pairs);
    }

    public Set<T> getKeySet() {
      return dist.keySet();
    }

    public Double getProbability(T key) {
      return dist.get(key);
    }


  }
  public static class Pair<K, V> {
    final K key;
    final V val;

    public Pair(K key, V val) {
      this.key = key;
      this.val = val;
    }
  }

}
