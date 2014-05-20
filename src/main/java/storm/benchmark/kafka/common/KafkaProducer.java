package storm.benchmark.kafka.common;

import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.metrics.BasicMetrics;
import storm.benchmark.util.KafkaUtils;
import storm.benchmark.util.Util;
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

    spoutNum = Util.retIfPositive(spoutNum, (Integer) options.get(SPOUT));
    boltNum = Util.retIfPositive(boltNum, (Integer) options.get(BOLT));

    metrics = new BasicMetrics();

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
    String brokers = (String) Util.retIfNotNull("localhost:9092", options.get(BROKER_LIST));
    String topic = (String) Util.retIfNotNull(KafkaUtils.DEFAULT_TOPIC, options.get(TOPIC));
    brokerConfig.put("metadata.broker.list", brokers);
    brokerConfig.put("serializer.class", "kafka.serializer.StringEncoder");
    brokerConfig.put("key.serializer.class", "kafka.serializer.StringEncoder");
    brokerConfig.put("request.required.acks", "1");
    kafkaConfig.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, brokerConfig);
    kafkaConfig.put(KafkaBolt.TOPIC, topic);
    return kafkaConfig;
  }

}
