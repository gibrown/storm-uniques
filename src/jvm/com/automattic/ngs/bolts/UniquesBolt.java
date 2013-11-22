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
import java.util.HashMap;
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
  protected static transient MySQLCommunicator hash_db = null;
  protected static transient MySQLCommunicator counts_db = null;
  protected transient RotatingMap<Long,Boolean> cache = null;

  protected String uniquesTableNamePrefix = null, countsTableNamePrefix = null;
  protected String dbURL = null, username = null, password = null;
  protected List<String> columnNames = null;
  protected List<String> keyTypes = null;
  protected HashMap<String,Boolean> tableExists = null;

  protected int timeBinSize = 1;
  protected String timeBinUnits = null;
  protected List<String> tupleFields = null;
      
  public UniquesBolt(
      String uniquesTableNamePrefix,
      String countsTableNamePrefix,
      String dbUrl,
      String username, 
      String password,
      int timeBinSize,
      String timeBinUnits,
      List<String> tupleFields
  ) throws SQLException {
    super();
    this.uniquesTableNamePrefix = uniquesTableNamePrefix;
    this.countsTableNamePrefix = countsTableNamePrefix;
    
    this.dbURL = dbUrl;
    this.username = username;
    this.password = password;

    this.timeBinSize = timeBinSize;
    this.timeBinUnits = timeBinUnits;
    this.tupleFields = tupleFields;
  }

  @Override
  public void prepare(java.util.Map stormConf, TopologyContext context) {
    this.tableExists = new HashMap<String, Boolean>();

    //cache tracks what has been seen by this bolt and reduces the load on the DB
    // works best if we group the tuples going to the bolts
    if ( null == this.cache ) {
      this.cache = new RotatingMap<Long,Boolean>(10);
		}

    //setup Hash DB for tracking what has been seen
    this.hash_db = new MySQLCommunicator(
      Arrays.asList("hash"),
      Arrays.asList("bigint")
    );

    try {
      this.hash_db.setupConnection(this.dbURL, this.username, this.password);
    } catch (Exception e) {
      throw new RuntimeException("Error connecting to DB: " + this.dbURL,  e);
    }

    //setup connection to the table(s) that will track counts
    this.counts_db = new MySQLCommunicator(
			Arrays.asList("blog_id","uniques"),
      Arrays.asList("bigint","bigint")
    );

    try {
      this.counts_db.setupConnection(this.dbURL, this.username, this.password);
    } catch (Exception e) {
      throw new RuntimeException("Error connecting to DB: " + this.dbURL,  e);
    }

	}


  //Assumptions about tuple fields:
  //  "time" is a Long timestamp
  //  "unique_id" is a Long
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    if (isTickTuple(tuple)) {
      this.cache.rotate(); //clean old data from cache
      return;
		}

    List<Object> hash_fields = getHashFields(tuple);
    Long time = tuple.getLongByField("time");
    Long time_bin = this.binTime( time );

    List<Object> all_fields = new ArrayList<Object>(hash_fields);
    all_fields.add(time_bin);
    long full_key = multiFieldHash( all_fields );

    if ( this.cache.containsKey( full_key ) )
      return;

    long key = multiFieldHash( hash_fields );

    if ( this.inUniquesTable( key, time_bin ) ) {
      this.cache.put( full_key, true );
      return; //not new
		}
    this.cache.put( full_key, true );

    this.incrementCountsTable( hash_fields, time_bin );

    System.out.println("New: " + tuple.toString());
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }

  @Override
  public void cleanup() {
    try {
      this.hash_db.closeConnection();
		} catch (Exception e ) {
      throw new RuntimeException("Error when closing hash db connection",  e);
		}

    try {
      this.counts_db.closeConnection();
		} catch (Exception e ) {
      throw new RuntimeException("Error when closing counts db connection",  e);
		}
	}

  protected List<Object> getHashFields( Tuple tuple ) {
    List<Object> lst = new ArrayList<Object>();
    for ( String fld : this.tupleFields ) {
      lst.add( tuple.getValueByField( fld ) );
		}
    return lst;
	}

  protected boolean inUniquesTable( Long hash, Long time_bin ) {
    String tblName = getTableName( this.uniquesTableNamePrefix, time_bin, this.timeBinUnits );
    if ( !this.tableExists.containsKey(tblName) ) {
      try {
        if ( !this.hash_db.tableExists( tblName ) ) {
          this.hash_db.createTable( tblName, "hash", "hash", Arrays.asList("hash"), Arrays.asList("bigint") );
  	  	}
      } catch(Exception e) {
        throw new RuntimeException("Error checking/creating hash DB table",  e);
      }
      this.tableExists.put( tblName, true );
		}

    List<Object> row = new ArrayList<Object>();
    row.add(hash);
    int rows = 0;
    try {
      rows = this.hash_db.insertRow( tblName, row, true );
    } catch(Exception e) {
      throw new RuntimeException("insertRow to hash table failed for: " + hash,  e);
    }

    return (rows == 0);
	}

  //This code is specific to calculating uniques per blog
  // assumes the hashFields are "blog_id", "unique_id" (in that order!)
  protected void incrementCountsTable( List<Object> vals, Long time_bin ) {
    String tblName = getTableName( this.countsTableNamePrefix, time_bin, this.timeBinUnits );
    if ( !this.tableExists.containsKey(tblName) ) {
      try {
        if ( !this.counts_db.tableExists( tblName ) ) {
          this.counts_db.createTable( tblName, "blog_id", "blog_id", Arrays.asList("blog_id","uniques"), Arrays.asList("bigint","bigint") );
  	  	}
      } catch(Exception e) {
        throw new RuntimeException("Error checking/creating counts DB table: " + tblName,  e);
      }
      this.tableExists.put( tblName, true );
		}

    List<Object> row = new ArrayList<Object>();
		row.add( vals.get(0) );  //blog_id
		row.add( 1 );
    int rows = 0;
    try {
      rows = this.counts_db.incrementRow( tblName, row, "uniques" );
    } catch(Exception e) {
      throw new RuntimeException("insertRow to hash table failed for: " + row.toString(),  e);
    }

	}

  protected static boolean isTickTuple(Tuple tuple) {
    return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
      && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
  }

  protected String getTableName( String prefix, long time_bin, String units ) {
    if ( units.equals( "second" ) ) {
      return prefix + "_" + time_bin;
		}
    throw new RuntimeException("Unsupported time bin units: " + units);
	}

  protected static long calcHash( Object obj ) {
    long h = 1125899906842597L; // prime

    if ( obj.getClass().getName().equals( "String" ) ) {
      String str = ((String)obj);
      int len = str.length();
    
      for (int i = 0; i < len; i++) {
        h = 31*h + str.charAt(i);
      }
      return h;
		}

    return obj.hashCode(); //default to 32 bit hashcode for non-strings
  }

  protected static long multiFieldHash( List<Object> fields ) {
    long hash = calcHash( fields.get(0) );
    for (int i=1; i < fields.size(); i++) {
      hash = hash * 31L + calcHash( fields.get(i) );
		}
    return hash;
	}

  protected long binTime( long timestamp ) {
    if ( this.timeBinUnits.equals("second") ) {
      return ( (long)(timestamp / this.timeBinSize) ) * this.timeBinSize;
		}

    throw new RuntimeException("Unsupported time bin units: " + this.timeBinUnits);
	}

}

