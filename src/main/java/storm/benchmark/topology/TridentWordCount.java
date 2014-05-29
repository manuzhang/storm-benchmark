package storm.benchmark.topology;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.trident.operation.WordSplit;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.KafkaUtils;
import storm.kafka.StringScheme;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.testing.MemoryMapState;

import java.util.Map;


public class TridentWordCount extends StormBenchmark {

  public static final String MAX_BATCH_SIZE = "trident.max.batch.size";
  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "topology.component.spout_num";
  public static final String SPLIT_ID = "split";
  public static final String SPLIT_NUM = "topology.component.split_bolt_num";
  public static final String COUNT_ID = "count";
  public static final String COUNT_NUM = "topology.component.count_bolt_num";

  private int maxBatchSize = 3;
  // number of spoutNum to run in parallel
  private int spoutNum = 8;
  // number of splitNum to run in parallel
  private int splitNum = 4;
  // number of countNum to run in parallel
  private int countNum = 4;

  private IPartitionedTridentSpout spout;

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);
    maxBatchSize = BenchmarkUtils.getInt(options, MAX_BATCH_SIZE, maxBatchSize);
    spoutNum = BenchmarkUtils.getInt(options, SPOUT_NUM, spoutNum);
    splitNum = BenchmarkUtils.getInt(options, SPLIT_NUM, splitNum);
    countNum = BenchmarkUtils.getInt(options, COUNT_NUM, countNum);

    spout  = new TransactionalTridentKafkaSpout(
            KafkaUtils.getTridentKafkaConfig(options, new SchemeAsMultiScheme(new StringScheme())));
    return this;
  }

  @Override
  public IBenchmark buildTopology() {
    TridentTopology trident = new TridentTopology();
    trident.newStream("wordcount", spout).parallelismHint(spoutNum).shuffle()
      .each(new Fields(StringScheme.STRING_SCHEME_KEY), new WordSplit(), new Fields("word")).parallelismHint(splitNum)
      .groupBy(new Fields("word"))
      .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count")).parallelismHint(countNum);

    topology = trident.build();

    return this;
  }


}
