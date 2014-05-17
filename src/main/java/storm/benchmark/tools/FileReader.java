package storm.benchmark.tools;

import storm.benchmark.util.FileUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FileReader implements Serializable {

  private static final long serialVersionUID = -7012334600647556267L;

  public final String file;
  private List<String> contents = new ArrayList<String>();
  private int index = 0;
	private int limit = 0;

  public FileReader(String file) {
    this.file= file;
    if (this.file != null) {
      this.contents = FileUtils.readLines(this.getClass().getResourceAsStream(this.file));
      this.limit = contents.size();
    } else {
      throw new IllegalArgumentException("file name cannot be null");
    }
  }

  public String nextLine() {
    if (index >= limit) {
	    index = 0;
	  }
    String line = contents.get(index);
    index++;
    return line;
  }


}
