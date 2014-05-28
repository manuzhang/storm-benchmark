package storm.benchmark.util;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.PrintWriter;

import static org.fest.assertions.api.Assertions.assertThat;

public class FileUtilsTest {

  private static final String PARENT = "./test/";
  private static final String PATH = PARENT + "file.txt";
  private static File dir;
  private static File file;
  private static PrintWriter writer;

  @BeforeTest
  public void setUp() {
    dir = new File(PARENT);
    file = new File(PATH);
    assertThat(dir).doesNotExist();
    assertThat(file).doesNotExist();
  }

  @AfterTest
  public void cleanUp() {
    writer.close();
    file.delete();
    assertThat(file).doesNotExist();
    dir.delete();
    assertThat(dir).doesNotExist();
  }

  @Test
  public void testFileCreateWhenParentDoesNotExist() {
    writer = FileUtils.createFileWriter(PARENT, PATH);
    assertThat(writer).isNotNull();
    assertThat(dir).exists();
    assertThat(file).exists();
  }

  @Test
  public void testFileCreateWhenParentExists() {
    dir.mkdirs();
    assertThat(dir).exists();
    writer = FileUtils.createFileWriter(PARENT, PATH);
    assertThat(writer).isNotNull();
    assertThat(file).exists();
  }

}
