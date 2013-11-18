package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.spout.RandomSentenceSpout;
import storm.starter.spout.RandomEventSpout;

import java.util.HashMap;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class UniquesTopology {


  public static class BlogUniques extends BaseBasicBolt {
    Map<Long, Map<String,Boolean>> blog_ips = new HashMap<Long, Map<String,Boolean>>();
    Map<Long, Long> blog_uniques = new HashMap<Long, Long>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
			String ip = tuple.getString(0);
      Long blog_id = tuple.getLong(1);

			Map<String, Boolean> blog_map = blog_ips.get(blog_id);
      if (blog_map == null) {
        blog_map = new HashMap<String,Boolean>();
				blog_ips.put(blog_id,blog_map);
			}
			Boolean seen = blog_map.get(ip);
			if (seen == null) {
				blog_map.put(ip,true);
				Long count = blog_uniques.get(blog_id);
				if (count == null )
					count = (long) 0;
				count++;
				blog_uniques.put(blog_id,count);
	      collector.emit(new Values(blog_id, count));
			}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("blog_id", "count"));
    }
  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new RandomEventSpout(), 5);

    builder.setBolt("count", new BlogUniques(), 12).fieldsGrouping("spout", new Fields("blog_id"));

    Config conf = new Config();
    conf.setDebug(true);


    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(10000);

      cluster.shutdown();
    }
  }
}
