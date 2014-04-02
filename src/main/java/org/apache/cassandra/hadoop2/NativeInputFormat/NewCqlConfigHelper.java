package org.apache.cassandra.hadoop2.NativeInputFormat;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;

import java.util.Collection;

public class NewCqlConfigHelper {
  private static final String INPUT_CQL_QUERY = "cassandra.input.query";

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

  public static void setInputCqlQuery(Configuration conf, String inputCqlQuery) {
    Preconditions.checkNotNull(inputCqlQuery);
    // TODO: Validate query is okay.
    conf.set(INPUT_CQL_QUERY, inputCqlQuery);
  }

  public static String getInputCqlQuery(Configuration conf) {
    return conf.get(INPUT_CQL_QUERY);
  }

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
