package storm.benchmark.util;

import backtype.storm.spout.MultiScheme;
import backtype.storm.utils.Utils;
import storm.kafka.BrokerHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TridentKafkaConfig;

import java.util.ArrayList;
import java.util.Map;

public final class KafkaUtils {

  public static final String ZOOKEEPER_SERVERS = "zookeeper.servers";
  public static final String KAFKA_ROOT_PATH = "kafka.root.path";
  public static final String TOPIC = "topic";
  public static final String CLIENT_ID = "client_id";

  public static final String DEFAULT_TOPIC = "storm";

  private KafkaUtils() {
  }

  public static SpoutConfig getSpoutConfig(Map options, MultiScheme scheme) throws IllegalArgumentException {
    String zkServers = (String) Utils.get(options, ZOOKEEPER_SERVERS, "localhost:2181");
    String kafkaRoot = (String) Utils.get(options, KAFKA_ROOT_PATH, "/kafka");
    String connectString = zkServers + kafkaRoot;

    BrokerHosts hosts = new ZkHosts(connectString);
    String topic = (String) Utils.get(options, TOPIC, DEFAULT_TOPIC);
    String zkRoot = kafkaRoot + "/" + "storm-consumer-states";
    String appId = (String) Utils.get(options, CLIENT_ID, "storm-app");

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

  public static TridentKafkaConfig getTridentKafkaConfig(Map options, MultiScheme scheme) {
    String zkServers = (String) Utils.get(options, ZOOKEEPER_SERVERS, "localhost:2181") ;
    String kafkaRoot = (String) Utils.get(options, KAFKA_ROOT_PATH, "/kafka");
    String connectString = zkServers + kafkaRoot;

    BrokerHosts hosts = new ZkHosts(connectString);
    String topic = (String) Utils.get(options, TOPIC, DEFAULT_TOPIC);
    String appId = (String) Utils.get(options, CLIENT_ID, "storm-app");

    TridentKafkaConfig config = new TridentKafkaConfig(hosts, topic, appId);
    config.scheme = scheme;
    return config;
  }
}
