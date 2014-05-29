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

  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "producer.spout_num";
  public static final String BOLT_ID = "bolt";
  public static final String BOLT_NUM = "producer.bolt_num";
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

    spoutNum = BenchmarkUtils.getInt(options, SPOUT_NUM, spoutNum);
    boltNum = BenchmarkUtils.getInt(options, BOLT_NUM, boltNum);

    return this;
  }

  @Override
  public IBenchmark buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(BOLT_ID, bolt, boltNum).localOrShuffleGrouping(SPOUT_ID);
    topology = builder.createTopology();
    return this;
  }

  @Override
  public IBenchmark startMetrics() {
    // don't run metrics for producers
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
