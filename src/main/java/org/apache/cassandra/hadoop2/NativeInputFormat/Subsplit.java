package org.apache.cassandra.hadoop2.NativeInputFormat;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Similar to an input split.
 */
public class Subsplit {
  // TODO: Add separate field for actual owner of token, versus replica nodes?
  String startToken;
  String endToken;
  Set<String> hosts;
  long estimatedNumberOfRows;

  public final static long NO_ESTIMATE = -1;
  public final static long RING_START_TOKEN = Long.MIN_VALUE;
  public final static long RING_END_TOKEN = Long.MAX_VALUE;

  public static Subsplit createFromHostSet(String startToken, String endToken, Set<String> hosts) {
    return new Subsplit(startToken, endToken, hosts);
  }

  public static Subsplit createFromHost(String startToken, String endToken, String host) {
    Set<String> hosts = Sets.newHashSet();
    hosts.add(host);
    return new Subsplit(startToken, endToken, hosts);
  }

  /**
   * Used only in unit tests.
   *
   * @param startToken
   * @param endToken
   * @param host
   * @param estimatedNumberOfRows
   * @return
   */
  static Subsplit createFromHost(
      String startToken,
      String endToken,
      String host,
      long estimatedNumberOfRows) {
    Set<String> hosts = Sets.newHashSet();
    hosts.add(host);
    return new Subsplit(startToken, endToken, hosts, estimatedNumberOfRows);
  }

  private Subsplit(String startToken, String endToken, Set<String> hosts) {
    this.startToken = startToken;
    this.endToken = endToken;
    this.hosts = hosts;
    estimatedNumberOfRows = NO_ESTIMATE;
  }

  private Subsplit(
      String startToken, String endToken, Set<String> hosts, long estimatedNumberOfRows) {
    this.startToken = startToken;
    this.endToken = endToken;
    this.hosts = hosts;
    this.estimatedNumberOfRows = estimatedNumberOfRows;
  }

  public String toString() {
    return String.format(
        "Subsplit from %s to %s @ %s",
        startToken,
        endToken,
        hosts
    );
  }

  public String getStartToken() {
    return startToken;
  }

  public String getEndToken() {
    return endToken;
  }

  public void setEstimatedNumberOfRows(long estimatedNumberOfRows) {
    this.estimatedNumberOfRows = estimatedNumberOfRows;
  }

  public long getEstimatedNumberOfRows() {
    return estimatedNumberOfRows;
  }

  public Set<String> getHosts() {
    return hosts;
  }

  public String getSortedHostListAsString() {
    List<String> hostList = new ArrayList(hosts);
    Collections.sort(hostList);
    return Joiner.on(",").join(hostList);
  }
}
