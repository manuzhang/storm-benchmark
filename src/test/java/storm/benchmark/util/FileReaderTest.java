package storm.benchmark.util;

import org.testng.annotations.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class FileReaderTest {
  private static final String FILE = "/resources/test.txt";

  @Test
  public void testNextLine() {
    FileReader reader = new FileReader(FILE);
    assertThat(reader.nextLine()).isEqualTo("first line");
    assertThat(reader.nextLine()).isEqualTo("second line");
    assertThat(reader.nextLine()).isEqualTo("third line");
    assertThat(reader.nextLine()).isEqualTo("first line");
  }

}
