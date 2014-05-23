package storm.benchmark.topology;

import backtype.storm.tuple.Fields;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.trident.operation.WordSplit;
import storm.benchmark.trident.spout.TridentFileReadSpout;
import storm.benchmark.util.BenchmarkUtils;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.MemoryMapState;

import java.util.Map;


public class TridentWordCount extends StormBenchmark {

  private static final String MAX_BATCH_SIZE="trident.max.batch.size";
  private static final String SPOUT = "spout1";
  private static final String SPLIT = "split1";
  private static final String COUNT = "count1";

  private int maxBatchSize = 3;
  // number of spoutNum to run in parallel
  private int spoutNum = 8;
  // number of splitNum to run in parallel
  private int splitNum = 4;
  // number of countNum to run in parallel
  private int countNum = 4;

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);
    maxBatchSize = BenchmarkUtils.getInt(options, MAX_BATCH_SIZE, maxBatchSize);

    spoutNum = BenchmarkUtils.getInt(options, SPOUT, spoutNum);
    splitNum = BenchmarkUtils.getInt(options, SPLIT, splitNum);
    countNum = BenchmarkUtils.getInt(options, COUNT, countNum);

    return this;
  }

  @Override
  public IBenchmark buildTopology() {


    TridentFileReadSpout spout = new TridentFileReadSpout(new Fields("sentence"), maxBatchSize);

    TridentTopology trident = new TridentTopology();
    trident.newStream(SPOUT, spout)
      .each(new Fields("sentence"), new WordSplit(), new Fields("word"))
      .groupBy(new Fields("word"))
      .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count")).parallelismHint(countNum);

    topology = trident.build();

    return this;
  }


}
