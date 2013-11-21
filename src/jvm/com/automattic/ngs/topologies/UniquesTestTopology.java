package com.automattic.ngs.topologies;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.ArrayList;

import com.automattic.ngs.bolts.UniquesBolt;
import com.automattic.ngs.spouts.RandomEventSpout;


/**
 * Test topology for building uniques processing.
 */
public class UniquesTestTopology {

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new RandomEventSpout(), 5);

    builder
      .setBolt("count", new UniquesBolt(
        "unique_id",
        "count",
        new ArrayList<String>(),
        new ArrayList<String>(),
        "jdbc:mysql://localhost/uniques",
        "sqluser",
        "sqluserpw"
      ), 2)
      .fieldsGrouping("spout", new Fields("unique_id","blog_id","location"))
		  ;

    Config conf = new Config();
    //conf.setDebug(true);
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("uniques", conf, builder.createTopology());

    }
  }
}
