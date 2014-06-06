package storm.benchmark.component.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import storm.benchmark.component.spout.pageview.PageView;
import storm.benchmark.util.MockTupleHelpers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static storm.benchmark.component.spout.pageview.PageView.Item;

public class PageViewBoltTest {

  @Test(dataProvider = "getAnyFields")
  public void shouldEmitOnExecute(Item field1, Item field2) {
    PageViewBolt bolt = new PageViewBolt(field1, field2);
    BasicOutputCollector collector = mock(BasicOutputCollector.class);
    Tuple input = MockTupleHelpers.mockAnyTuple();
    when(input.getString(0)).thenReturn("http://view1\t200\t100000\t100");
    bolt.execute(input, collector);
    verify(collector, times(1)).emit(any(Values.class));
  }

  @Test(dataProvider = "getAnyFields")
  public void shouldDeclareOutputFields(Item field1, Item field2) {
    PageViewBolt bolt = new PageViewBolt(field1, field2);
    OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
    bolt.declareOutputFields(declarer);
    verify(declarer, times(1)).declare(any(Fields.class));
  }

  @Test(dataProvider =  "getAnyFields")
  public void getValueShouldReturnRightField(Item field1, Item field2) {
    PageViewBolt bolt = new PageViewBolt(field1, field2);

    PageView  view1 = new PageView("http://view1", 200, 100000, 100);
    assertEquals(view1.toString(), view1.getValue(Item.ALL));
    assertEquals("http://view1", view1.getValue(Item.URL));
    assertEquals(200, view1.getValue(Item.STATUS));
    assertEquals(100000, view1.getValue(Item.ZIP));
    assertEquals(100, view1.getValue(Item.USER));
    assertEquals(1, view1.getValue(Item.ONE));

    PageView  view2 = new PageView("http://view2", 400, 200000, 200);
    assertEquals(view2.toString(), view2.getValue(Item.ALL));
    assertEquals("http://view2", view2.getValue(Item.URL));
    assertEquals(400, view2.getValue(Item.STATUS));
    assertEquals(200000, view2.getValue(Item.ZIP));
    assertEquals(200, view2.getValue(Item.USER));
    assertEquals(1, view2.getValue(Item.ONE));
  }

  @DataProvider
  private Object[][] getAnyFields() {
    final Item[] items = Item.values();
    final int num = items.length;
    final Item[][] fields = new Item[(num - 1) * num][2];
    int index = 0;
    for (int i = 0; i < num; i++) {
      for (int j = 0; j < num; j++) {
        if (j != i) {
          fields[index][0] = items[i];
          fields[index][1] = items[j];
          index++;
        }
      }
    }
    return fields;
  }
}
