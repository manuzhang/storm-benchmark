package storm.benchmark.component.spout.pageview;

import org.apache.log4j.Logger;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class PageView {
  private static Logger LOG = Logger.getLogger(PageView.class);
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
    return String.format("%s\t%d\t%d\t%d", url, status, zipCode, userID);
  }

  public static PageView fromString(String pv) {
    LOG.debug("get string '" + pv + "'");
    String[] parts = pv.split("\t");
    if (parts.length < 4) {
      return null;
    }
    for (int i = 0; i < parts.length; i++) {
      parts[i] = parts[i].trim();
    }
    return new PageView(parts[0],
            Integer.parseInt(parts[1]),
            Integer.parseInt(parts[2]),
            Integer.parseInt(parts[3]));
  }

  public Object getValue(Item field) {
    switch (field) {
      case URL:
        return url;
      case STATUS:
        return status;
      case ZIP:
        return zipCode;
      case USER:
        return userID;
      case ONE:
        return 1;
      default:
        return toString();
    }
  }

  public static enum Item {
    ALL("page_view"),
    URL("url"),
    STATUS("http_status"),
    ZIP("zip_code"),
    USER("user_id"),
    ONE("count_one");

    private final String name;

    Item(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  public static class Extract extends BaseFunction {

    private final List<Item> fields;

    public Extract(List<Item> fields) {
      this.fields = fields;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String pvString = tuple.getString(0);
      PageView pageView = PageView.fromString(pvString);
      if (null == pageView) {
        LOG.error("invalid pageview string '" + pvString + "'");
        return;
      }
      List<Object> values = new ArrayList<Object>();
      for (Item field : fields) {
        values.add(pageView.getValue(field));
      }
      collector.emit(values);
    }
  }
}
