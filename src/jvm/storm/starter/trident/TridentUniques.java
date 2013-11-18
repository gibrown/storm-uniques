package storm.starter.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.Aggregator;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.util.ArrayList;

import storm.starter.spout.RandomEventSpout;


public class TridentUniques {
  /**
   * A filter that filters nothing but prints the tuples it sees. Useful to test and debug things.
   */
  @SuppressWarnings({ "serial", "rawtypes" })
  public static class PrintFilter implements Filter {

          @Override
          public void prepare(Map conf, TridentOperationContext context) {
          }
          @Override
          public void cleanup() {
          }

          @Override
          public boolean isKeep(TridentTuple tuple) {
                  System.out.println(tuple);
                  return true;
          }
  }

  public static class MakeUniqueId implements Filter {

          @Override
          public void prepare(Map conf, TridentOperationContext context) {
          }
          @Override
          public void cleanup() {
          }

          @Override
          public boolean isKeep(TridentTuple tuple) {
                  System.out.println(tuple);
                  return true;
          }
  }


  public static class BinTime extends BaseFunction {
    int binSize;
    String binUnits; //eg second, day, week, month

	  public BinTime( Integer size, String units  ) {
      binSize = size;
			binUnits = units;
	  }

    public void execute(TridentTuple tuple, TridentCollector collector) {
      //TODO: calc time offsets correctly
      long time = tuple.getLong(0);
      long val = ( (long)(time / binSize) ) * binSize;
      collector.emit(new Values(val));
    }
  }

  public static class One implements CombinerAggregator<String> {
    public String init(TridentTuple tuple) {
      return "1";
    }
  
    public String combine(String val1, String val2) {
      return "1";
    }
  
    public String zero() {
      return "1";
    }
  }

  /**
   * A simple Aggregator that produces a hashmap of key, counts.
   */
  @SuppressWarnings("serial")
  public static class LocationAggregator implements Aggregator<Map<String, Long>> {
    int partitionId;

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
      this.partitionId = context.getPartitionIndex();
    }

    @Override
    public void cleanup() {
    }

    @Override
    public Map<String, Long> init(Object batchId, TridentCollector collector) {
      return new HashMap<String, Long>();
    }

    @Override
    public void aggregate(Map<String, Long> map, TridentTuple tuple, TridentCollector collector) {
      String id = tuple.getString(0);
			Long val = map.get(id);
			if ( null == val )
				val = 0L;
			val++;
      map.put(id, val);
    }

    @Override
    public void complete(Map<String, Long> val, TridentCollector collector) {
      System.err.println("I am partition [" + partitionId + "] and have aggregated: [" + val + "]");
      collector.emit(new Values(val));
    }
  }

  public static StormTopology buildTopology() throws IOException {
    RandomEventSpout spout = new RandomEventSpout();

		//simple test
    //TridentTopology topology = new TridentTopology();
    //topology.newStream("spout", spout).each(new Fields("blog_id"),
    //  new PrintFilter());

		//simple unique counting
    // TridentTopology topology = new TridentTopology();
    // topology.newStream("spout", spout).parallelismHint(16)
		// 	.groupBy(new Fields("ip"))
		// 	.aggregate(new Fields("ip","blog_id"), new LocationAggregator(), new Fields("ip_counts"))
    //     .parallelismHint(16)
    //     .each(new Fields("ip_counts"), new PrintFilter());

		// //uniques by blog id
    // TridentTopology topology = new TridentTopology();
    // topology.newStream("spout", spout).parallelismHint(16)
		// 	.groupBy(new Fields("blog_id"))
		// 	.aggregate(new Fields("ip"), new LocationAggregator(), new Fields("ip_counts"))
		// 	.each(new Fields("ip_counts","blog_id"), new PrintFilter());

		//uniques across all
    TridentTopology topology = new TridentTopology();

    addUniqueProcessing( new ArrayList(), 1, "second", topology, spout );

    return topology.build();
  }

	//add a uniques processing task to the topology
	public static void addUniqueProcessing( ArrayList<String> unique_fields, int time_bin_size, String time_bin_unit, TridentTopology topology, RandomEventSpout spout ) {
    ArrayList<String> group = new ArrayList();
    group.add("time_bin");
    group.add("unique_id");
    //group.add(unique_fields);

    topology.newStream("spout", spout) //blog_id,unique_id,location,time
      .project(new Fields( "time_bin","unique_id")) //reduce number of fields we send in
      .each(new Fields("time"), new BinTime( time_bin_size, time_bin_unit ), new Fields("time_bin"))
			.groupBy(new Fields("time_bin","unique_id")) //TODO: fix me by adding unique_fields parameters!
      .persistentAggregate(new MemoryMapState.Factory(), new One(), new Fields("one"))
      .groupBy(new Fields("time_bin"))
      .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("unique_count"))
			.each(new Fields("unique_count"), new PrintFilter()); //TODO: this should write to a DB

	}

  public static void main(String[] args) throws Exception {
    Config conf = new Config();
    StormTopology topology = buildTopology();

    if ( args != null && args.length > 0 ) {
      conf.setNumWorkers( 3 );
      StormSubmitter.submitTopology( args[0], conf, topology );
    } else {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology( "hackaton", conf, topology );
    }
  }
}
