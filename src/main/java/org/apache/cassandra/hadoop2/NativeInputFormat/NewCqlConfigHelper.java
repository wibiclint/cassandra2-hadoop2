package org.apache.cassandra.hadoop2.NativeInputFormat;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;

public class NewCqlConfigHelper {

  // TODO: Figure out if the table name should include a keyspace.

  // TODO: Put all of the query information together into an object.
  // Try using the Hadoop DefaultStringifier to serialize these objects.

  // -----------------------------------------------------------------------------------------------
  // Stuff for keeping track of different CQL queries.
  private static final String INPUT_CQL_QUERY_COUNTER = "cassandra.input.query.count";
  private static final String INPUT_CQL_QUERY_KEYSPACE = "cassandra.input.query.keyspace";
  private static final String INPUT_CQL_QUERY_TABLE = "cassandra.input.query.table";
  private static final String INPUT_CQL_QUERY_COLUMNS = "cassandra.input.query.columns";
  private static final String INPUT_CQL_QUERY_WHERE_CLAUSES = "cassandra.input.query.where.clauses";

  // TODO: User can specify a list of columns to use for saying what rows should be together.

  // TODO: Check that the primary keys (or at least partition keys) are same for each table.

  // TODO: Provide a builder API for specifying all of the different queries?

  public static void setInputCqlQuery(
      Configuration conf,
      String keyspace,
      String table,
      String columnsCsv,
      String whereClauses) {
    // Get the current query count and increment it.
    int currentQueryCount = conf.getInt(INPUT_CQL_QUERY_COUNTER, 0);
    conf.setInt(INPUT_CQL_QUERY_COUNTER, currentQueryCount+1);

    // Store the collection of columns and the where clause.
    conf.set(INPUT_CQL_QUERY_KEYSPACE + Integer.toString(currentQueryCount), keyspace);
    conf.set(INPUT_CQL_QUERY_TABLE + Integer.toString(currentQueryCount), table);
    conf.set(INPUT_CQL_QUERY_COLUMNS + Integer.toString(currentQueryCount), columnsCsv);
    conf.set(INPUT_CQL_QUERY_WHERE_CLAUSES + Integer.toString(currentQueryCount), whereClauses);
  }

  public static void setInputCqlQuery(
      Configuration conf,
      String keyspace,
      String table,
      String columnsCsv) {
    setInputCqlQuery(conf, keyspace, table, columnsCsv, "");
  }

  public static List<CqlQuerySpec> getInputCqlQueries(Configuration conf) {
    List<CqlQuerySpec> queries = Lists.newArrayList();
    for (int i = 0; i < conf.getInt(INPUT_CQL_QUERY_COUNTER, 0); i++) {
      String keyspace = conf.get(INPUT_CQL_QUERY_KEYSPACE + Integer.toString(i));
      String table = conf.get(INPUT_CQL_QUERY_TABLE + Integer.toString(i));
      String columnsCsv = conf.get(INPUT_CQL_QUERY_COLUMNS + Integer.toString(i));
      String whereClauses = conf.get(INPUT_CQL_QUERY_WHERE_CLAUSES + Integer.toString(i));
      queries.add(new CqlQuerySpec(keyspace, table, columnsCsv, whereClauses));
    }
    return queries;
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
