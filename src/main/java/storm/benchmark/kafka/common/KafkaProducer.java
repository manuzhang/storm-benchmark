package storm.benchmark.kafka.common;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import storm.benchmark.StormBenchmark;
import storm.benchmark.metrics.IMetricsCollector;
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

  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_BOLT_NUM = 4;

  protected IRichSpout spout;
  protected final IRichBolt bolt = new KafkaBolt<String, String>();

  @Override
  public StormTopology getTopology(Config config) {
    config.putAll(getKafkaConfig(config));

    final int spoutNum = BenchmarkUtils.getInt(config , SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int boltNum = BenchmarkUtils.getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(BOLT_ID, bolt, boltNum).localOrShuffleGrouping(SPOUT_ID);
    return builder.createTopology();
  }

  @Override
  public IMetricsCollector getMetricsCollector(Config config, StormTopology topology) {
    return new IMetricsCollector() {
      @Override
      public void collect() {
        // we do not collect metrics for kafka producers
      }
    };
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
