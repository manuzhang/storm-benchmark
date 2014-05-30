package storm.benchmark.tools;


import java.io.Serializable;
import java.util.Random;

import static storm.benchmark.tools.Distribution.Pair;


/* port from https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/streaming/examples/clickstream/PageViewGenerator.scala */

public class PageViewGenerator implements Serializable {

  private static final long serialVersionUID = 6825565414146438901L;
  public static final Distribution<String> PAGES = new Distribution<String>(new Pair("foo.com", 0.7),
                                                            new Pair("foo.news.com", 0.2),
                                                            new Pair("foo.contact.com", 0.1));
  public static final Distribution<Integer> HTTP_STATUS = new Distribution<Integer>(new Pair(200, 0.95), new Pair(404, 0.05));
  public static final Distribution<Integer> USER_ZIP_CODE = new Distribution<Integer>(new Pair(94709, 0.5), new Pair(94117, 0.5));
  public static final Distribution<Integer> USER_ID = Distribution.intEvenDistribution(1, 100);

  public String getNextClickEvent() {
    String page = pickFromDistribution(PAGES);
    int status = pickFromDistribution(HTTP_STATUS);
    int zip = pickFromDistribution(USER_ZIP_CODE);
    int id = pickFromDistribution(USER_ID);
    return new PageView(page, status, zip, id).toString();
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



}
