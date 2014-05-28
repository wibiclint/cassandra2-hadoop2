package org.apache.cassandra.hadoop2.multiquery;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigHelper {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigHelper.class);

  /** This is just a static utility class. */
  private ConfigHelper() {
    throw new AssertionError();
  }

  // -----------------------------------------------------------------------------------------------
  // Stuff for keeping track of different CQL queries.
  private static final String INPUT_CQL_QUERY_COUNTER = "cassandra.input.query.count";
  private static final String INPUT_CQL_QUERY_QUERY = "cassandra.input.query.query";

  public static void setInputCqlQuery(Configuration conf, CqlQuerySpec query) {
    Preconditions.checkNotNull(conf);
    // Get the current query count and increment it.
    int currentQueryCount = conf.getInt(INPUT_CQL_QUERY_COUNTER, 0);
    conf.setInt(INPUT_CQL_QUERY_COUNTER, currentQueryCount+1);

    String queryIndex = Integer.toString(currentQueryCount);

    // Serialize the query.
    final String serializedRequest = Base64.encodeBase64String(SerializationUtils.serialize(query));
    conf.set(INPUT_CQL_QUERY_QUERY + queryIndex, serializedRequest);
  }

  public static List<CqlQuerySpec> getInputCqlQueries(Configuration conf) {
    Preconditions.checkNotNull(conf);
    List<CqlQuerySpec> queries = Lists.newArrayList();
    for (int i = 0; i < conf.getInt(INPUT_CQL_QUERY_COUNTER, 0); i++) {
      String serializedQuery = conf.get(INPUT_CQL_QUERY_QUERY + i);
      Preconditions.checkNotNull(serializedQuery);
      CqlQuerySpec query = SerializationUtils.deserialize(Base64.decodeBase64(serializedQuery));
      queries.add(query);
    }
    return queries;
  }

  // -----------------------------------------------------------------------------------------------
  // We allow you to specify additional columns (beyond the partition key) to use for grouping
  // together rows.
  private static final String INPUT_CQL_QUERY_CLUSTERING_COLUMNS =
      "cassandra.input.query.clustering.columns";

  public static void setInputCqlQueryClusteringColumns(Configuration conf, String... columns) {
    Preconditions.checkNotNull(conf);
    String columnCsv = Joiner.on(",").join(columns);
    LOG.info("Setting clustering columns " + columnCsv);
    conf.set(INPUT_CQL_QUERY_CLUSTERING_COLUMNS, columnCsv);
  }

  public static List<String> getInputCqlQueryClusteringColumns(Configuration conf) {
    Preconditions.checkNotNull(conf);
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
    Preconditions.checkNotNull(conf);
    conf.setInt(INPUT_CQL_PAGE_ROW_SIZE, pageRowSize);
  }

  public static int getInputCqlPageRowSize(Configuration conf) {
    Preconditions.checkNotNull(conf);
    return conf.getInt(INPUT_CQL_PAGE_ROW_SIZE, DEFAULT_INPUT_CQL_PAGE_ROW_SIZE);
  }

  public static void setInputNativeTransportPort(Configuration conf, int port) {
    Preconditions.checkNotNull(conf);
    conf.setInt(INPUT_NATIVE_TRANSPORT_PORT, port);
  }

  public static int getInputNativeTransportPort(Configuration conf) {
    Preconditions.checkNotNull(conf);
    return conf.getInt(INPUT_NATIVE_TRANSPORT_PORT, DEFAULT_INPUT_NATIVE_TRANSPORT_PORT);
  }

  public static void setInputNativeTransportContactPoints(Configuration conf, String... contacts) {
    Preconditions.checkNotNull(conf);
    Preconditions.checkArgument(contacts.length> 0);
    for (String contact : contacts) {
      Preconditions.checkNotNull(contact);
    }
    conf.setStrings(INPUT_NATIVE_TRANSPORT_CONTACT_POINTS, contacts);
  }

  public static String[] getInputNativeTransportContactPoints(Configuration conf) {
    Preconditions.checkNotNull(conf);
    String[] contacts = conf.getStrings(INPUT_NATIVE_TRANSPORT_CONTACT_POINTS);
    if (null != contacts && contacts.length > 0) {
      return contacts;
    }
    return new String[] {DEFAULT_INPUT_NATIVE_TRANSPORT_CONTACT_POINT};
  }

  public static void setInputTargetNumSplits(Configuration conf, int numSplits) {
    Preconditions.checkNotNull(conf);
    conf.setInt(INPUT_TARGET_NUM_SPLITS, numSplits);
  }

  public static int getDefaultInputTargetNumSplits(Configuration conf) {
    Preconditions.checkNotNull(conf);
    return conf.getInt(INPUT_TARGET_NUM_SPLITS, DEFAULT_INPUT_TARGET_NUM_SPLITS);
  }
}
