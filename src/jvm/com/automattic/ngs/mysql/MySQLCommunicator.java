package com.automattic.ngs.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;

/*
 * Class implementing methods for communicating(create a table, check for a table's existence, insert a row) with RDBMS
 */
public class MySQLCommunicator {

  private transient Connection con = null;
  private String dbUrl = null;
  private String dbClass = "com.mysql.jdbc.Driver";
  private boolean result = false;
  private String primaryKey = null, tableName = null;
  private List<String> columnNames = new ArrayList<String>(); 
  private List<String> columnTypes = new ArrayList<String>();

  public MySQLCommunicator(
      String primaryKey, 
      String tableName,
      List<String> columnNames, 
      List<String> columnTypes
  ) {
    super();
    this.primaryKey = primaryKey;
    this.primaryKey = primaryKey;
    this.tableName = tableName;
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
  }

  /*
   * get connection and return a Connection object
   */
  public void setupConnection(final String sqlDBUrl, final String sqlUser, final String sqlPassword) throws ClassNotFoundException, SQLException {
    
    StringBuilder builder = new StringBuilder();
    builder.append(sqlDBUrl).append("?user=").append(sqlUser).append("&password=").append(sqlPassword);
    dbUrl = builder.toString();
    DriverManager.registerDriver(new com.mysql.jdbc.Driver());
    Class.forName(dbClass);
    this.con = DriverManager.getConnection (dbUrl);
  }

  public void closeConnection() throws Exception {
    if ( null != this.con )
      this.con.close();
	}

  //check for table's existence 
  public boolean tableExists(String tableName) {
    ResultSet rs = null;
    PreparedStatement prepstmt = null;
    String stmt = null;
    try {
      prepstmt = null;
      stmt = "SELECT * FROM " + tableName + " LIMIT 1";
      //System.out.println(stmt);
      prepstmt = con.prepareStatement(stmt);
      rs = prepstmt.executeQuery();
      if(rs.next()) {
        result = true;
      } else {
        result = false;                                
      }
    } catch(Exception e) {
      result = false;
    }
    return result;
  }

  //create a table in RDBMS
  public void createTable(String tableName, String primaryKey, String uniqueKey, List<String> columnNames, List<String> columnTypes) throws SQLException {
    PreparedStatement prepstmt = null;
    String stmt = null;
    String colType = null;
    int noOfColumns = 0;
    int r = 0;
    try {                
      prepstmt = null;
      stmt = "CREATE TABLE " + tableName + "(";
      noOfColumns = columnNames.size();
      colType = null;
      if(columnNames.size() == columnTypes.size()) {
        for(int i = 0; i < noOfColumns - 1; i++) {
          colType = columnTypes.get(i);
          stmt = stmt + columnNames.get(i) + " " + colType + ",";        
        }
        stmt = stmt + columnNames.get(noOfColumns-1) + " " + columnTypes.get(noOfColumns-1);
      } else {
        System.out.println("Wrong input : Number of columns doesn't match the number of given data types");
      }
      if(!primaryKey.equals("N/A")) {
        stmt = stmt + ", PRIMARY KEY (" + primaryKey + ")";
      }
      if(!uniqueKey.equals("N/A")) {
        stmt = stmt + ", Unique KEY (" + uniqueKey + ")";
      }
      stmt = stmt + ") ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1";
      //System.out.println(stmt);
      prepstmt = con.prepareStatement(stmt);
      r = prepstmt.executeUpdate();
      if(r != 0) {
        return;
      }
                    
    }
    catch(Exception e) {
      //table already exists
    }
  }

  public int insertRow(List<Object> fieldValues) throws SQLException {
    return this.insertRow(fieldValues,false);
	}

  //insert a row in the RDBMS table 
  public int insertRow(List<Object> fieldValues, boolean ignore) throws SQLException {
    int r = 0;
    String stmt = null;
    PreparedStatement prepstmt = null;
    int noOfColumns = 0;
    try {
      prepstmt = null;
      noOfColumns = columnNames.size();
      if ( ignore )
        stmt = "INSERT IGNORE INTO ";
      else
        stmt = "INSERT INTO ";
      stmt += tableName + this.buildInsertColumns( fieldValues );
      r = this.con.createStatement().executeUpdate( stmt );
      //prepstmt = con.prepareStatement(stmt);
      //for(int j = 0; j <= noOfColumns - 1; j++) {
      //  prepstmt.setObject(j + 1, fieldValues.get(j));
      //}
      //System.out.println("Values: " + fieldValues.toString() );
      //System.out.println(prepstmt.toString());
      //r = prepstmt.executeUpdate();
      if(r == 0) {
        return 0;
      }
    }
    catch(Exception e) {
      //TODO: handle errors
      if ( null == prepstmt )
        System.out.println( "prepstmt is null" );
      else
        System.out.println( prepstmt.toString() );
      if ( null == this.con )
        System.out.println( "con is null" );
      else
        System.out.println( this.con.toString() );
      System.out.println( fieldValues.toString() );
      System.out.println( stmt );
      e.printStackTrace();
    }
    return r;
  }

  //insert a row in the RDBMS table 
  private String buildInsertColumns(List<Object> fieldValues) {
    String text = " (";
    String values = "";
    int noOfColumns = columnNames.size(); 
    for(int i = 0; i <= noOfColumns - 1; i++) {
      Object v = fieldValues.get(i);
      String vs = null;
      if ( "String" == v.getClass().getName() )
        vs = "`" + v + "`";
      else
        vs = v.toString();
      if(i != noOfColumns - 1) {
        text = text + "`" + columnNames.get(i) + "`, ";
        //values = values + "?,";
        values = values + vs + ",";
      }
      else {
        text = text + "`" + columnNames.get(i) + "`) ";
        //values = values + "?";
        values = values + vs;
      }
    }
    text = text + " VALUES (" +  values + ")";
    return text;
	}

}
