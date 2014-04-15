package storm.benchmark.topology;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.benchmark.IBenchmark;
import storm.benchmark.StormBenchmark;
import storm.benchmark.metrics.TridentMetrics;
import storm.benchmark.spout.TridentFileReadSpout;
import storm.benchmark.util.Util;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

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
    spout.setCycle(true);

    TridentTopology trident = new TridentTopology();
    trident.newStream(SPOUT, spout).parallelismHint(spouts)
      .each(new Fields("sentence"), new Split(), new Fields("word")).parallelismHint(splits)
      .groupBy(new Fields("word"))
      .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count")).parallelismHint(counts);

    topology = trident.build();

    return this;
  }

  public static class Split extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String sentence = tuple.getString(0);
      for (String word : sentence.split(" ")) {
        collector.emit(new Values(word));
      }
    }
  }
}
