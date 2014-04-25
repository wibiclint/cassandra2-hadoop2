package org.apache.cassandra.hadoop2.NativeInputFormat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

/**
 * Class representing a smallest-possible range of tokens that share replica nodes.
 *
 * This class essentially maps to a Cassandra virtual node (vnode).
 */
public class Subsplit {
  // TODO: Add separate field for actual owner of token, versus replica nodes?
  String startToken; // inclusive
  String endToken; // inclusive
  Set<String> hosts;

  public final static long RING_START_TOKEN = Long.MIN_VALUE;
  public final static long RING_END_TOKEN = Long.MAX_VALUE;

  /**
   * Create a subsplit given a token range and a set of replica nodes.
   *
   * @param startToken The minimum token for the subsplit (inclusive).
   * @param endToken The maximum token for the subsplit (inclusive).
   * @param hosts A set of replica nodes for this token range.
   * @return A new subsplit for this token range.
   */
  public static Subsplit createFromHostSet(String startToken, String endToken, Set<String> hosts) {
    return new Subsplit(startToken, endToken, hosts);
  }

  /**
   * Create a subsplit given a token range and a set of replica nodes.
   *
   * @param startToken The minimum token for the subsplit (inclusive).
   * @param endToken The maximum token for the subsplit (inclusive).
   * @param host The master node for this token range.
   * @return A new subsplit for this token range.
   */
  public static Subsplit createFromHost(String startToken, String endToken, String host) {
    Set<String> hosts = Sets.newHashSet();
    hosts.add(host);
    return new Subsplit(startToken, endToken, hosts);
  }

  /**
   * Private constructor for a subsplit.
   *
   * @param startToken The minimum token for the subsplit (inclusive).
   * @param endToken The maximum token for the subsplit (inclusive).
   * @param hosts A set of replica nodes for this token range.
   */
  private Subsplit(String startToken, String endToken, Set<String> hosts) {
    this.startToken = startToken;
    this.endToken = endToken;
    this.hosts = hosts;
  }

  /** {@inheritDoc} */
  public String toString() {
    return String.format(
        "Subsplit from %s to %s @ %s",
        startToken,
        endToken,
        hosts
    );
  }

  /**
   * Getter for the minimum token value for this subsplit.
   *
   * @return The minimum token value for this subsplit.
   */
  public String getStartToken() {
    return startToken;
  }

  /**
   * Getter for the maximum token value for this subsplit.
   *
   * @return The maximum token value for this subsplit.
   */
  public String getEndToken() {
    return endToken;
  }

  /**
   * Getter for the replica nodes for this subsplit.
   *
   * @return The replica nodes for this subsplit.
   */
  public Set<String> getHosts() {
    return hosts;
  }

  /**
   * Get a comma-separated list of the hosts for this subsplit.
   *
   * @return A CSV of hosts, as a string.
   */
  public String getSortedHostListAsString() {
    List<String> hostList = new ArrayList(hosts);
    Collections.sort(hostList);
    return Joiner.on(",").join(hostList);
  }
}
