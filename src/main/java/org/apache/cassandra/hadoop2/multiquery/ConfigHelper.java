package org.apache.cassandra.hadoop2.multiquery;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;

public class ConfigHelper {
  /** This is just a static utility class. */
  private ConfigHelper() {
    throw new AssertionError();
  }

  // -----------------------------------------------------------------------------------------------
  // Stuff for keeping track of different CQL queries.
  private static final String INPUT_CQL_QUERY_COUNTER = "cassandra.input.query.count";
  private static final String INPUT_CQL_QUERY_KEYSPACE = "cassandra.input.query.keyspace";
  private static final String INPUT_CQL_QUERY_TABLE = "cassandra.input.query.table";
  private static final String INPUT_CQL_QUERY_COLUMNS = "cassandra.input.query.columns";
  private static final String INPUT_CQL_QUERY_WHERE_CLAUSES = "cassandra.input.query.where.clauses";

  public static void setInputCqlQuery(Configuration conf, CqlQuerySpec query) {
    // Get the current query count and increment it.
    int currentQueryCount = conf.getInt(INPUT_CQL_QUERY_COUNTER, 0);
    conf.setInt(INPUT_CQL_QUERY_COUNTER, currentQueryCount+1);

    String queryIndex = Integer.toString(currentQueryCount);

    // Store the collection of columns and the where clause.
    conf.set(INPUT_CQL_QUERY_KEYSPACE + queryIndex, query.getKeyspace());
    conf.set(INPUT_CQL_QUERY_TABLE + queryIndex, query.getTable());
    conf.set(INPUT_CQL_QUERY_COLUMNS + queryIndex, query.getColumnCsv());
    conf.set(INPUT_CQL_QUERY_WHERE_CLAUSES + queryIndex, query.getWhereClauses());
  }

  public static List<CqlQuerySpec> getInputCqlQueries(Configuration conf) {
    List<CqlQuerySpec> queries = Lists.newArrayList();
    for (int i = 0; i < conf.getInt(INPUT_CQL_QUERY_COUNTER, 0); i++) {
      String keyspace = conf.get(INPUT_CQL_QUERY_KEYSPACE + Integer.toString(i));
      String table = conf.get(INPUT_CQL_QUERY_TABLE + Integer.toString(i));
      String columnsCsv = conf.get(INPUT_CQL_QUERY_COLUMNS + Integer.toString(i));
      String whereClause = conf.get(INPUT_CQL_QUERY_WHERE_CLAUSES + Integer.toString(i));
      queries.add(CqlQuerySpec.builder()
          .withKeyspace(keyspace)
          .withTable(table)
          .withColumnsCsv(columnsCsv)
          .withWhereClause(whereClause)
          .build());

    }
    return queries;
  }

  // -----------------------------------------------------------------------------------------------
  // We allow you to specify additional columns (beyond the partition key) to use for grouping
  // together rows.
  private static final String INPUT_CQL_QUERY_CLUSTERING_COLUMNS =
      "cassandra.input.query.clustering.columns";

  public static void setInputCqlQueryClusteringColumns(Configuration conf, String... columns) {
    conf.set(INPUT_CQL_QUERY_CLUSTERING_COLUMNS, Joiner.on(",").join(columns));
  }

  public static List<String> getInputCqlQueryClusteringColumns(Configuration conf) {
    String csv = conf.get(INPUT_CQL_QUERY_CLUSTERING_COLUMNS, null);
    if (null == csv) {
      return new ArrayList<String>();
    }
    return Splitter.on(",").splitToList(csv);
  }
  // -----------------------------------------------------------------------------------------------
  // Everything else.

  private static final String INPUT_CQL_PAGE_ROW_SIZE = "cassandra.input.page.row.size";
  private static final int DEFAULT_INPUT_CQL_PAGE_ROW_SIZE = 100;

  private static final String INPUT_NATIVE_TRANSPORT_PORT = "cassandra.input.native.port";
  private static final int DEFAULT_INPUT_NATIVE_TRANSPORT_PORT = 9042;

  private static final String INPUT_NATIVE_TRANSPORT_CONTACT_POINTS =
      "cassandra.input.native.contacts";
  private static final String DEFAULT_INPUT_NATIVE_TRANSPORT_CONTACT_POINT = "127.0.0.1";

  private static final String INPUT_TARGET_NUM_SPLITS = "cassandra.input.num_splits";
  private static final int DEFAULT_INPUT_TARGET_NUM_SPLITS = 1;

  // TODO: keyspace, consistency level

  public static void setInputCqlPageRowSize(Configuration conf, int pageRowSize) {
    conf.setInt(INPUT_CQL_PAGE_ROW_SIZE, pageRowSize);
  }

  public static int getInputCqlPageRowSize(Configuration conf) {
    return conf.getInt(INPUT_CQL_PAGE_ROW_SIZE, DEFAULT_INPUT_CQL_PAGE_ROW_SIZE);
  }

  public static void setInputNativeTransportPort(Configuration conf, int port) {
    conf.setInt(INPUT_NATIVE_TRANSPORT_PORT, port);
  }

  public static int getDefaultInputNativeTransportPort(Configuration conf) {
    return conf.getInt(INPUT_NATIVE_TRANSPORT_PORT, DEFAULT_INPUT_NATIVE_TRANSPORT_PORT);
  }

  public static void setInputNativeTransportContactPoints(Configuration conf, String... contacts) {
    Preconditions.checkArgument(contacts.length> 0);
    conf.setStrings(INPUT_NATIVE_TRANSPORT_CONTACT_POINTS, contacts);
  }

  public static String[] getInputNativeTransportContactPoints(Configuration conf) {
    String[] contacts = conf.getStrings(INPUT_NATIVE_TRANSPORT_CONTACT_POINTS);
    if (contacts.length > 0) {
      return contacts;
    }
    return new String[] {DEFAULT_INPUT_NATIVE_TRANSPORT_CONTACT_POINT};
  }

  public static void setInputTargetNumSplits(Configuration conf, int numSplits) {
    conf.setInt(INPUT_TARGET_NUM_SPLITS, numSplits);
  }

  public static int getDefaultInputTargetNumSplits(Configuration conf) {
    return conf.getInt(INPUT_TARGET_NUM_SPLITS, DEFAULT_INPUT_TARGET_NUM_SPLITS);
  }
}
