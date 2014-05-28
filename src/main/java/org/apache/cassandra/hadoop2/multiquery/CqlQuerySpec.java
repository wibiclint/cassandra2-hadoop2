package org.apache.cassandra.hadoop2.multiquery;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
* Specifies a Cassandra query to perform in a Hadoop RecordReader.
*/
public class CqlQuerySpec implements Serializable {
  private final String mKeyspace;
  private final String mTable;
  private final List<String> mColumns;
  private final WhereClause mWhereClauses;

  private static final WhereClause EMPTY_WHERE_CLAUSE = null;

  public static final String ALL_COLUMNS = "*";

  public String toString() {
    return String.format("CqlQuerySpec: SELECT %s FROM %s.%s WHERE %s",
        mKeyspace,
        mTable,
        mColumns,
        mWhereClauses
    );
  }

  private CqlQuerySpec(
      String keyspace, String table, List<String> columns, WhereClause whereClause) {
    mKeyspace = keyspace;
    mTable = table;
    mColumns = columns;
    mWhereClauses = whereClause;
  }

  public String getTable() {
    return mTable;
  }

  public String getKeyspace() {
    return mKeyspace;
  }

  public String getColumnCsv() {
    if (mColumns.size() == 0) {
      return ALL_COLUMNS;
    }
    return Joiner.on(",").join(mColumns);
  }

  public WhereClause getWhereClauses() {
    return mWhereClauses;
  }

  public static CqlQuerySpecBuilder builder() {
    return new CqlQuerySpecBuilder();
  }

  public static class CqlQuerySpecBuilder {
    private String mKeyspace;
    private String mTable;
    private List<String> mColumns;
    private WhereClause mWhereClause;

    public CqlQuerySpecBuilder() {
      mKeyspace = null;
      mTable = null;
      mColumns = Lists.newArrayList();
      mWhereClause = null;
    }

    public CqlQuerySpecBuilder withKeyspace(String keyspace) {
      Preconditions.checkNotNull(keyspace);
      if (mKeyspace != null) {
        throw new IllegalArgumentException("You can specify only one keyspace.");
      }
      mKeyspace = keyspace;
      return this;
    }

    public CqlQuerySpecBuilder withTable(String table) {
      if (mTable != null) {
        throw new IllegalArgumentException("You can specify only one table.");
      }
      Preconditions.checkNotNull(table);
      mTable = table;
      return this;
    }

    public CqlQuerySpecBuilder withColumns(String ... columns) {
      Preconditions.checkArgument(columns.length > 0);
      mColumns.addAll(Arrays.asList(columns));
      return this;
    }

    CqlQuerySpecBuilder withColumnsCsv(String columnsCsv) {
      Preconditions.checkNotNull(columnsCsv);
      if (columnsCsv.equals(ALL_COLUMNS)) {
        return this;
      }
      mColumns.addAll(Lists.newArrayList(Splitter.on(",").split(columnsCsv)));
      return this;
    }

    public CqlQuerySpecBuilder withWhereClause(String rawWhereClause, Serializable... args) {
      if (mWhereClause != null) {
        throw new IllegalArgumentException(
            "You can specify only one where clause (although that clause can have multiple " +
                "conditions combined with ANDs).");
      }
      Preconditions.checkNotNull(rawWhereClause);
      /*
      if (rawWhereClause.equals(EMPTY_WHERE_CLAUSE)) {
        Preconditions.checkArgument(args.length == 0);
        return this;
      }
      */
      if (!rawWhereClause.toLowerCase().contains("where")) {
        throw new IllegalArgumentException("WHERE clause must contain 'WHERE'");
      }
      mWhereClause = new WhereClause(rawWhereClause, Lists.newArrayList(args));
      return this;
    }

    public CqlQuerySpec build() {
      if (null == mKeyspace) {
        throw new IllegalArgumentException("You must specify a keyspace.");
      }
      if (null == mTable) {
        throw new IllegalArgumentException("You must specify a table.");
      }
      if (null == mWhereClause) {
        mWhereClause = EMPTY_WHERE_CLAUSE;
      }

      return new CqlQuerySpec(mKeyspace, mTable, mColumns, mWhereClause);

    }

  }

}
