package storm.benchmark.util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public final class FileUtils {

  private FileUtils() {
  }

  public static List<String> readLines(InputStream input) {
    List<String> lines = new ArrayList<String>();
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(input));
      try {
        String line;
        while((line = reader.readLine()) != null) {
          lines.add(line);
        }
      } catch (IOException e) {
        throw new RuntimeException("Reading file failed", e);
      } finally {
        reader.close();
      }
    } catch (IOException e) {
      throw new RuntimeException("Error closing reader", e);
    }
    return lines;
  }

  public static PrintWriter createFileWriter(String parent, String name) {
    try {
      final File dir = new File(parent);
      if (dir.exists() || dir.mkdirs()) {
        final File file = new File(name);
        file.createNewFile();
        final PrintWriter writer = new PrintWriter(new OutputStreamWriter(
        new FileOutputStream(file, true)));
        return writer;
      } else {
        throw new RuntimeException("fail to create parent directory " + parent);
      }
    } catch (IOException e) {
      throw new RuntimeException("No such file or directory " + name, e);
    }
  }
}
