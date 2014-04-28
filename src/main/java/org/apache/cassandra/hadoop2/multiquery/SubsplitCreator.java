package org.apache.cassandra.hadoop2.multiquery;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class responsible for creating subsplits.
 */
class SubsplitCreator {
  private static final Logger LOG = LoggerFactory.getLogger(SubsplitCreator.class);

  private final Configuration mConf;

  private final Session mSession;

  /**
   * Constructor for SubsplitCreator.
   * @param conf Hadoop configuration containing information about the Cassandra cluster.
   */
  public SubsplitCreator(Configuration conf, Session session) {
    this.mConf = conf;
    mSession = session;

    // Check that this session uses the load-balancing policy that we need.
    Preconditions.checkArgument(
        session.getCluster().getConfiguration().getPolicies().getLoadBalancingPolicy()
            instanceof ConsistentHostOrderPolicy
    );
  }

  /**
   * Create a list of subsplits for this Cassandra cluster.  Each subsplit contains an IP address
   * and a token range.
   * @return The subsplits.
   */
  public List<Subsplit> createSubsplits() {
    // Create subsplits that initially contain a mapping from token ranges to primary hosts.
    List<Subsplit> subsplits = createInitialSubsplits();

    // TODO: Add replica nodes to the subsplits.

    return subsplits;
  }

  private List<Subsplit> createInitialSubsplits() {
    // Retrieve all of the tokens for each host from the system tables.
    Map<String, String> tokensToMasterNodes = getTokenToMasterNodeMapping(mConf);

    // Go from a mapping between tokens and hosts to a mapping between token *ranges* and hosts.
    List<Long> sortedTokens = Lists.newArrayList();
    for (String tok : tokensToMasterNodes.keySet()) {
      sortedTokens.add(Long.parseLong(tok));
    }
    Collections.sort(sortedTokens);
    LOG.debug(String.format("Found %d total tokens", sortedTokens.size()));
    LOG.debug(String.format("Minimum tokens is %s", sortedTokens.get(0)));
    LOG.debug(String.format("Maximum tokens is %s", sortedTokens.get(sortedTokens.size() - 1)));

    // We need to add the global min and global max token values so that we make sure that our
    // subsplits cover all of the data in the cluster.
    sortedTokens.add(Subsplit.RING_START_TOKEN);
    sortedTokens.add(Subsplit.RING_END_TOKEN);
    Collections.sort(sortedTokens);
    assert(sortedTokens.get(0) == Subsplit.RING_START_TOKEN);
    assert(sortedTokens.get(sortedTokens.size()-1) == Subsplit.RING_END_TOKEN);

    // Loop through all of the pairs of tokens, creating subsplits for every pair.  Remember in
    // C* that the master node for a token gets all data between the *previous* token and the token
    // in question, so we assign ownership of a given subsplit to the node associated with the
    // second (greater) of the two tokens.
    List<Subsplit> subsplits = Lists.newArrayList();

    for (int tokenIndex = 0; tokenIndex < sortedTokens.size() - 1; tokenIndex++) {
      long lowerBoundToken = sortedTokens.get(tokenIndex);
      // Ownership for a given node looks like (previous token, my token], so we add 1 to the
      // start token, unless the start token is the first token in our entire ring.
      if (tokenIndex > 0) {
        lowerBoundToken++;
      }
      String startToken = Long.toString(lowerBoundToken);
      String endToken = sortedTokens.get(tokenIndex + 1).toString();

      String hostForEndToken = tokensToMasterNodes.get(endToken);
      if (tokenIndex == sortedTokens.size() - 2) {
        assert(null == hostForEndToken);
        hostForEndToken = tokensToMasterNodes.get(startToken);
      }
      assert (null != hostForEndToken);

      Subsplit subsplit = Subsplit.createFromHost(startToken, endToken, hostForEndToken);
      subsplits.add(subsplit);
    }
    return subsplits;
  }

  /**
   * Read metadata from our Cassandra cluster to get the mapping from tokens to master nodes.
   *
   * @param conf Hadoop configuration containing information about the Cassandra cluster.
   * @return A map from tokens (stored as strings) to the master nodes (stored as IP addresses).
   */
  private Map<String, String> getTokenToMasterNodeMapping(Configuration conf) {
    Map<String, String> tokensToMasterNodes = Maps.newHashMap();

    // Get the set of tokens for the local host.
    updateTokenListForLocalHost(mSession, tokensToMasterNodes);

    // Query the `local` and `peers` tables to get mappings from tokens to hosts.
    updateTokenListForPeers(mSession, tokensToMasterNodes);

    return tokensToMasterNodes;
  }

  /**
   * Update our map of tokens to master nodes by getting a list of tokens owned by the local host.
   * @param session An open Cassandra session.
   * @param tokensToHosts The map from tokens to master nodes to update.
   */
  private void updateTokenListForLocalHost(
      Session session,
      Map<String, String> tokensToHosts) {
    String queryString = "SELECT tokens FROM system.local;";
    ResultSet resultSet = session.execute(queryString);
    List<Row> results = resultSet.all();
    Preconditions.checkArgument(results.size() == 1);

    Set<String> tokens = results.get(0).getSet("tokens", String.class);

    updateTokenListForSingleNode("localhost", tokens, tokensToHosts);
  }

  /**
   * Update our map of tokens to master nodes by getting a list of tokens owned by peers.
   * @param session An open Cassandra session.
   * @param tokensToHosts The map from tokens to master nodes to update.
   */
  private void updateTokenListForPeers(
      Session session,
      Map<String, String> tokensToHosts) {

    String queryString = "SELECT rpc_address, tokens FROM system.peers;";
    ResultSet resultSet = session.execute(queryString);

    for (Row row : resultSet.all()) {
      Set<String> tokens = row.getSet("tokens", String.class);
      InetAddress rpcAddress = row.getInet("rpc_address");
      String hostName = rpcAddress.getHostName();
      assert (!hostName.equals("localhost"));
      updateTokenListForSingleNode(hostName, tokens, tokensToHosts);
    }
  }

  /**
   * Given a host and a list of tokens for which the host is the master node, update our map of
   * tokens to master nodes.
   * @param hostName The name of the host.
   * @param tokens A list of tokens for which the host is the master node.
   * @param tokensToHosts The map from tokens to master nodes to update.
   */
  private void updateTokenListForSingleNode(
      String hostName,
      Set<String> tokens,
      Map<String, String> tokensToHosts) {

    LOG.debug(String.format("Got %d tokens for host %s", tokens.size(), hostName));

    // For every token, create an entry in the map from that token to the host.
    for (String token : tokens) {
      assert (!tokensToHosts.containsKey(token));
      tokensToHosts.put(token, hostName);
    }
  }
}
