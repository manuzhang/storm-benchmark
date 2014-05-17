package storm.benchmark.util;

import backtype.storm.spout.MultiScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.ArrayList;
import java.util.Map;

public final class KafkaUtils {

  public static final String ZOOKEEPER_SERVERS = "zookeeper.servers";
  public static final String KAFKA_ROOT_PATH = "kafka.root.path";
  public static final String TOPIC = "topic";
  public static final String APP_ID = "id";

  private KafkaUtils() {
  }

  public static SpoutConfig getKafkaSpoutConfig(Map options, MultiScheme scheme) throws IllegalArgumentException {
    String zkServers = (String) Util.retIfNotNull("localhost:2181", options.get(ZOOKEEPER_SERVERS));
    String kafkaRoot = (String) Util.retIfNotNull("/kafka", options.get(KAFKA_ROOT_PATH));
    String connectString = zkServers + kafkaRoot;

    BrokerHosts hosts = new ZkHosts(connectString);
    String topic = (String) Util.retIfNotNull("storm", options.get(TOPIC));
    String zkRoot = kafkaRoot + "/" + "storm-consumer-states";
    String appId = (String) Util.retIfNotNull("storm-app", options.get(APP_ID));

    SpoutConfig config = new SpoutConfig(hosts, topic, zkRoot, appId);
    config.zkServers = new ArrayList<String>();

    String [] servers = zkServers.split(",");

    for (int i = 0; i < servers.length; i++) {
      String[] serverAndPort = servers[0].split(":");
      config.zkServers.add(serverAndPort[0]);
      int port = Integer.parseInt(serverAndPort[1]);
      if (i == 0) {
        config.zkPort = port;
      }

      if (config.zkPort != port) {
        throw new IllegalArgumentException("The zookeeper port on all  server must be same");
      }
    }
    config.scheme = scheme;
    return config;
  }
}
