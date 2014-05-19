package storm.benchmark.kafka.common;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.benchmark.util.Util;
import storm.kafka.bolt.KafkaBolt;

import java.util.Map;

/**
 * KafkaProducerSpout generates source data for downstream KafkaBolt to
 * write into Kafka. The output fields consist of BOLT_KEY and BOLT_MESSAGE.
 * BOLT_KEY will decide the Kafka partition to write into and BOLT_MESSAGE the
 * actual message. Users set the number of partitions and by default messages will
 * be written into each partition in a round-robin way.
 */
public abstract class KafkaProducerSpout extends BaseRichSpout {

  private static final long serialVersionUID = -3823006007489002720L;
  protected int partitions;
  private int index;
  protected SpoutOutputCollector collector;

  public KafkaProducerSpout(int partitions) {
    this.partitions = Util.retIfPositive(1, partitions);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(KafkaBolt.BOLT_KEY, KafkaBolt.BOLT_MESSAGE));
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.collector = collector;
  }

  protected void nextMessage(String message) {
    if (index >= partitions) {
      index = 0;
    }
    collector.emit(new Values(index + "", message));
    index++;
  }
}
