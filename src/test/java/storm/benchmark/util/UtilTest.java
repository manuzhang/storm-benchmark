package storm.benchmark.util;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(JUnitParamsRunner.class)
public class UtilTest {


  /* test method retIfPositive */

  private final int DEF_RET = 1;

  @Test
  @Parameters(method = "getPositive")
  public void positiveShouldReturn(int posVal) {
    assertEquals(posVal, Util.retIfPositive(DEF_RET, posVal));
  }

  @Test
  @Parameters(method = "getNonPositive")
  public void nonPositiveReturnDefault(int nonPosVal) {
    assertEquals(DEF_RET, Util.retIfPositive(DEF_RET, nonPosVal));
  }

  private Object[] getPositive() {
    return new Integer[][]{
            {3}, {5}, {2}
    };
  }

  private Object[] getNonPositive() {
    return new Integer[][] {
            {0}, {-1}, {-100}, {null}
    };
  }

  /* test method putIfAbsent */

  private final String DEF_KEY = "foo";
  private final String DEF_VAL = "bar";

  @Test
  @Parameters(method = "getDataWithNewKey")
  public void NewKeyShouldBePutIn(String newKey, String val) {
    Map<String, String> map = new HashMap<String, String>();
    map.put(DEF_KEY, DEF_VAL);
    Util.putIfAbsent(map, newKey, val);
    assertTrue(map.containsKey(newKey));
    String ret = map.get(newKey);
    if (val != null) {
      assertTrue(val.equals(ret));
    } else {
      assertTrue(null == ret);
    }
  }

  @Test
  @Parameters(method = "getDataWithOldKey")
  public void OldKeyShouldBeSkipped(String oldKey, String val) {
    Map<String, String> map  = new HashMap<String, String>();
    map.put(DEF_KEY, DEF_VAL);
    Util.putIfAbsent(map, oldKey, val);
    assertTrue(map.containsKey(oldKey));
    String ret = map.get(oldKey);
    assertNotNull(ret);
    assertFalse(ret.equals(val));
  }

  private Object[] getDataWithNewKey() {
    return new String[][] {
            {"foo1", "bar1"},
            {"foo2", "bar2"},
            {"foo3", null}
    };
  }

  private Object[] getDataWithOldKey() {
    return new String[][] {
            {DEF_KEY, "bar1"},
            {DEF_KEY, "bar2"},
            {DEF_KEY, null}
    };
  }
}
