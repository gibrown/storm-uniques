package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class RandomEventSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Random _rand;


  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
  }

  @Override
  public void nextTuple() {
		String unique_id = Integer.toString( _rand.nextInt(100000) );
		long blog_id = _rand.nextInt(100) + 1;

    String[] locations = new String[]{ "US", "UK", "AU", "NZ", "CR" };
    String location = locations[_rand.nextInt(locations.length)];
		long time = System.currentTimeMillis() / 1000L;
    _collector.emit(new Values(unique_id, blog_id, location, time));
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("unique_id","blog_id","location","time"));
  }

}