package org.apache.cassandra.hadoop2.NativeInputFormat;

/**
* Created by clint on 4/25/14.
*/
public class CqlQuerySpec {
  private final String mKeyspace;
  private final String mTable;
  private final String mColumnsCsv;
  private final String mWhereClauses;

  public CqlQuerySpec(String keyspace, String table, String columnsCsv, String whereClauses) {
    mKeyspace = keyspace;
    mTable = table;
    mColumnsCsv = columnsCsv;
    mWhereClauses = whereClauses;
  }

  public String getTable() {
    return mTable;
  }

  public String getKeyspace() {
    return mKeyspace;
  }

  public String getColumnCsv() {
    return mColumnsCsv;
  }

  public String getWhereClauses() {
    return mWhereClauses;
  }

}
