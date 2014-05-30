package storm.benchmark.tools;

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
  }
}
