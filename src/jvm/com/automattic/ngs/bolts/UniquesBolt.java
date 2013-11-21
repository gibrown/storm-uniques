package com.automattic.ngs.bolts;

import backtype.storm.Constants;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RotatingMap;
import java.util.Map;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.sql.SQLException;

import com.automattic.ngs.mysql.MySQLCommunicator;


// Stores a concatenation of fields, hashed together into a table that
// consists only of a single column
// the data is inserted with INSERT IGNORE
@SuppressWarnings("serial")
public class UniquesBolt extends BaseBasicBolt {
  private static transient MySQLCommunicator db = null;
  private transient RotatingMap<Integer,Boolean> cache = null;

  private String uniquesTableName = null, countsTableName = null;
  private String dbURL = null, username = null, password = null;
  private List<String> columnNames = null;
  private List<String> keyTypes = null;
      
  public UniquesBolt(
      String uniquesTableName,
      String countsTableName,
      List<String> keyColumnNames,
      List<String> keyColumnTypes,
      String dbUrl,
      String username, 
      String password
  ) throws SQLException {
    super();
    this.uniquesTableName = uniquesTableName;
    this.countsTableName = countsTableName;
    this.columnNames = keyColumnNames;
    this.keyTypes = keyColumnTypes;
    
    this.dbURL = dbUrl;
    this.username = username;
    this.password = password;
  }

  @Override
  public void prepare(java.util.Map stormConf, TopologyContext context) {
    if ( null == this.cache ) {
      this.cache = new RotatingMap<Integer,Boolean>(10);
		}

    this.db = new MySQLCommunicator(
      "hash", 
      this.uniquesTableName, 
      Arrays.asList("hash"),
      Arrays.asList("bigint")
    );

    try {
      this.db.setupConnection(this.dbURL, this.username, this.password);
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      if ( !this.db.tableExists( this.uniquesTableName ) ) {
        this.db.createTable( this.uniquesTableName, "hash", "hash", Arrays.asList("hash"), Arrays.asList("bigint") );
	  	}
    } catch(Exception e) {
      e.printStackTrace();
    }
	}

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    if (isTickTuple(tuple)) {
      this.cache.rotate(); //clean old data from cache
      return;
		}

    String hash_string = "";
    for( int i=0; i < 1; i++ ) {
      hash_string += "-" + tuple.getValue(i).toString();
		}
    int key = hash_string.hashCode();

    if ( this.cache.containsKey( key ) )
      return;

    List<Object> row = new ArrayList<Object>();
    row.add(key);
    int rows = 0;
    try {
      rows = this.db.insertRow( row, true );
      this.cache.put( key, true );
    } catch(Exception e) {
      System.out.println( "insertRow failed for " + key );
      //e.printStackTrace();
    }

    if ( rows == 0 )
      return; //not new already in DB

    System.out.println("New: " + tuple.toString());
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }

  @Override
  public void cleanup() {
    try {
      this.db.closeConnection();
		} catch (Exception e ) {
      e.printStackTrace();
		}
	}

  private static boolean isTickTuple(Tuple tuple) {
    return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
      && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
  }
}

