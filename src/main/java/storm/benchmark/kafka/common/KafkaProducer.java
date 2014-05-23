package storm.benchmark.kafka.common;

import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.KafkaUtils;
import storm.kafka.bolt.KafkaBolt;

import java.util.HashMap;
import java.util.Map;

/**
 * KafkaProducer is itself a Storm topology which consists of a KafkaProducerSpout and a KafkaBolt
 * Subclass could provide its own Spout (e.g. read from file, generate message randomly)
 */
public abstract class KafkaProducer extends StormBenchmark {

  public static final String SPOUT = "spout";
  public static final String BOLT = "bolt";
  public static final String BROKER_LIST = "broker.list";
  public static final String TOPIC = "topic";

  // number of spoutNum to run in parallel
  protected int spoutNum = 4;
  // number of boltNum to run in parallel
  protected int boltNum = 4;

  protected IRichSpout spout;
  protected final IRichBolt bolt = new KafkaBolt<String, String>();

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);

    Map stormConfig = config.getStormConfig();
    stormConfig.putAll(getKafkaConfig(options));

    spoutNum = BenchmarkUtils.getInt(options, SPOUT, spoutNum);
    boltNum = BenchmarkUtils.getInt(options, BOLT, boltNum);

    return this;
  }

  @Override
  public IBenchmark buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT, spout, spoutNum);
    builder.setBolt(BOLT, bolt, boltNum).localOrShuffleGrouping(SPOUT);
    topology = builder.createTopology();
    return this;
  }

  private Map getKafkaConfig(Map options) {
    Map kafkaConfig = new HashMap();
    Map brokerConfig = new HashMap();
    String brokers = (String) Utils.get(options, BROKER_LIST, "localhost:9092");
    String topic = (String) Utils.get(options, TOPIC, KafkaUtils.DEFAULT_TOPIC);
    brokerConfig.put("metadata.broker.list", brokers);
    brokerConfig.put("serializer.class", "kafka.serializer.StringEncoder");
    brokerConfig.put("key.serializer.class", "kafka.serializer.StringEncoder");
    brokerConfig.put("request.required.acks", "1");
    kafkaConfig.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, brokerConfig);
    kafkaConfig.put(KafkaBolt.TOPIC, topic);
    return kafkaConfig;
  }

}
