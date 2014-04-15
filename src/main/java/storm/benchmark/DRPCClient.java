package storm.benchmark;

import backtype.storm.utils.Utils;

import java.util.Map;

public class DRPCClient {

  private static final String FUNCTION = "drpc.function";
  private static final String ARG = "drpc.arg";
  private static final String SERVER = "drpc.server";
  private static final String PORT = "drpc.port";

  public static void main(String[] args) throws Exception {
    Map options = Utils.readCommandLineOpts();
    String function = null;
    String arg = null;
    String server = "localhost";
    int port = 3772;

    if (options.containsKey(FUNCTION)) {
      function = (String) options.get(FUNCTION);
    } else {
      throw new IllegalArgumentException("must set a function for drpc");
    }

    if (options.containsKey(ARG)) {
      arg = (String) options.get(ARG);
    } else {
      throw new IllegalArgumentException("must set an arg for drpc");
    }

    if (options.containsKey(SERVER)) {
      server = (String) options.get(SERVER);
    } else {
      throw new IllegalArgumentException("must set a drpc server");
    }

    if (options.containsKey(PORT)) {
      port = (Integer) options.get(PORT);
    } else {
      throw new IllegalArgumentException("must set a drpc port");
    }

    backtype.storm.utils.DRPCClient client = new backtype.storm.utils.DRPCClient(server, port);
    String result = client.execute(function, arg);
    System.out.println(String.format("%s(\"%s\") = %s", function, arg, result));
  }
}
