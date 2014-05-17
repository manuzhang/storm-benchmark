package storm.benchmark.tools;


import java.io.Serializable;
import java.util.Random;

import static storm.benchmark.tools.Distribution.Pair;


/* port from https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/streaming/examples/clickstream/PageViewGenerator.scala */

public class PageViewGenerator implements Serializable {

  private static final long serialVersionUID = 6825565414146438901L;
  public static final Distribution<String> PAGES = new Distribution<String>(new Pair("http://foo.com/", 0.7),
                                                            new Pair("http://foo.com/news", 0.2),
                                                            new Pair("http://foo.com/contact", 0.1));
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

  public static class PageView {
    public final String url;
    public final int status;
    public final int zipCode;
    public final int userID;

    public PageView(String url, int status, int zipCode, int userID) {
      this.url = url;
      this.status = status;
      this.zipCode = zipCode;
      this.userID = userID;
    }

    @Override
    public String toString() {
      return String.format("%s\t%d\t%d\t%d\n", url, status, zipCode, userID);
    }

    public static PageView fromString(String s) {
      String[] parts = s.split("\t");
      if (parts.length < 4) {
        return null;
      }
      return new PageView(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), Integer.parseInt(parts[3]));
    }
  }


}
