package storm.benchmark.util;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.testng.Assert.*;

public class UtilTest {

  private static final int DEF_POS = 1;
  private static final String DEF_OBJ = "default_object";

  /* test method retIfPositive */

  @Test(dataProvider = "getPositive")
  public void positiveShouldReturn(int posVal) {
    assertEquals(posVal, Util.retIfPositive(DEF_POS, posVal));
  }

  @Test(dataProvider = "getNonPositive")
  public void nonPositiveReturnDefault(int nonPosVal) {
    assertEquals(DEF_POS, Util.retIfPositive(DEF_POS, nonPosVal));
  }

  @DataProvider
  private Object[][] getPositive() {
    return new Integer[][]{
            {3}, {5}, {2}
    };
  }

  @DataProvider
  private Object[][] getNonPositive() {
    return new Integer[][] {
          //  {0}, {-1}, {-100}, {null}
          {0}, {-1}, {-100}
    };
  }

  /* test method retIfNotNull */
  @Test
  public void nullValReturnDefault() {
    assertEquals(DEF_OBJ, Util.retIfNotNull(DEF_OBJ, null));
  }

  @Test(dataProvider = "getNonNull")
  public void nonNullshouldReturn(Object nonNul) {
    assertEquals(nonNul, Util.retIfNotNull(DEF_OBJ, nonNul));
  }

  @DataProvider
  public Object[][] getNonNull() {
    return new Object[][] {
            {"foo"}, {"bar"}
    };
  }

  /* test method putIfAbsent */

  private final String DEF_KEY = "foo";
  private final String DEF_VAL = "bar";

  @Test(dataProvider = "getDataWithNewKey")
  public void NewKeyShouldBePutIn(String newKey, String val) {
    Map<String, String> map = new HashMap<String, String>();
    map.put(DEF_KEY, DEF_VAL);
    Util.putIfAbsent(map, newKey, val);
    assertThat(map).containsKey(newKey);
    String ret = map.get(newKey);
    if (val != null) {
      assertEquals(val, ret);
    } else {
      assertNull(ret);
    }
  }

  @Test(dataProvider = "getDataWithOldKey")
  public void OldKeyShouldBeSkipped(String oldKey, String val) {
    Map<String, String> map  = new HashMap<String, String>();
    map.put(DEF_KEY, DEF_VAL);
    Util.putIfAbsent(map, oldKey, val);
    assertThat(map).containsKey(oldKey);
    String ret = map.get(oldKey);
    assertNotNull(ret);
    assertNotEquals(ret, val);
  }

  @DataProvider
  private Object[][] getDataWithNewKey() {
    return new String[][] {
            {"foo1", "bar1"},
            {"foo2", "bar2"},
            {"foo3", null}
    };
  }

  @DataProvider
  private Object[][] getDataWithOldKey() {
    return new String[][] {
            {DEF_KEY, "bar1"},
            {DEF_KEY, "bar2"},
            {DEF_KEY, null}
    };
  }
}
