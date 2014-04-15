package storm.benchmark.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class SOLBolt extends BaseBasicBolt {
  private String field = "message";

  public SOLBolt() {
  }

  public SOLBolt(String field) {
    this.field = field;
  }

  @Override
  public void prepare(Map conf, TopologyContext context) {
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    collector.emit(new Values(tuple.getString(0)));
  }

  @Override
  public void cleanup() {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(field));
  }
}