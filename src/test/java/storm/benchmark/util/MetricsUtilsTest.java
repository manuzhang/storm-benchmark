package storm.benchmark.util;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.TopologySummary;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetricsUtilsTest {

  @Test
  public void testAddLatency() {
    Map<String, List<Double>> stats = Maps.newHashMap();
    String id = "spout";
    double lat = 0.01;
    MetricsUtils.addLatency(stats, id, lat);
    assertThat(stats).containsKey(id);
    assertThat(stats.get(id)).isNotNull().contains(lat);
  }

  @Test
  public void testGetTopologySummary() {
    ClusterSummary cs = mock(ClusterSummary.class);
    TopologySummary ts = mock(TopologySummary.class);
    String tsName = "topology";
    String fakeName = "fake";

    when(cs.get_topologies()).thenReturn(Lists.newArrayList(ts));
    when(ts.get_name()).thenReturn(tsName);

    assertThat(MetricsUtils.getTopologySummary(cs, tsName)).isEqualTo(ts);
    assertThat(MetricsUtils.getTopologySummary(cs, fakeName)).isNull();
  }

}
