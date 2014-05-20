package storm.benchmark.topology;

import backtype.storm.tuple.Fields;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.metrics.TridentMetrics;
import storm.benchmark.trident.operation.WordSplit;
import storm.benchmark.trident.spout.TridentFileReadSpout;
import storm.benchmark.util.Util;
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
  // number of spouts to run in parallel
  private int spouts = 8;
  // number of splits to run in parallel
  private int splits = 4;
  // number of counts to run in parallel
  private int counts = 4;

  @Override
  public IBenchmark parseOptions(Map options) {
    super.parseOptions(options);
     if (options.containsKey(MAX_BATCH_SIZE)) {
      maxBatchSize = Util.retIfPositive(maxBatchSize, (Integer) options.get(MAX_BATCH_SIZE));
    }

    spouts = Util.retIfPositive(spouts, (Integer) options.get(SPOUT));
    splits = Util.retIfPositive(splits, (Integer) options.get(SPLIT));
    counts = Util.retIfPositive(counts, (Integer) options.get(COUNT));

    metrics = new TridentMetrics();

    return this;
  }

  @Override
  public IBenchmark buildTopology() {


    TridentFileReadSpout spout = new TridentFileReadSpout(new Fields("sentence"), maxBatchSize);

    TridentTopology trident = new TridentTopology();
    trident.newStream(SPOUT, spout).parallelismHint(spouts)
      .each(new Fields("sentence"), new WordSplit(), new Fields("word")).parallelismHint(splits)
      .groupBy(new Fields("word"))
      .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count")).parallelismHint(counts);

    topology = trident.build();

    return this;
  }


}
