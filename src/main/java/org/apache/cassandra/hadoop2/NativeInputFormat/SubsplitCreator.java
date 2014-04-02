package org.apache.cassandra.hadoop2.NativeInputFormat;

import com.datastax.driver.core.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.hadoop2.ConfigHelper;
import org.apache.cassandra.hadoop2.cql3.CqlConfigHelper;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.*;

/**
 * Class responsible for creating subsplits.
 */
public class SubsplitCreator {
  private static final Logger LOG = LoggerFactory.getLogger(SubsplitCreator.class);

  private final Configuration conf;

  /** Fraction of subsplits to use for sampling estimated number of rows / subsplit. */
  private static final double SUBPLIT_SAMPLE_FRACTION = 0.1;

  public SubsplitCreator(Configuration conf) {
    this.conf = conf;
  }

  public Set<Subsplit> createSubsplits() {
    // Create subsplits that initially contain a mapping from token ranges to primary hosts.
    Set<Subsplit> subsplits = createInitialSubsplits();

    // TODO: Add replica nodes to the subsplits.

    return subsplits;
  }

  private Set<Subsplit> createInitialSubsplits() {
    // Retrieve all of the tokens for each host from the system tables.
    Map<String, String> tokensToMasterNodes = getTokenToMasterNodeMapping(conf);

    // Go from a mapping between tokens and hosts to a mapping between token *ranges* and hosts.
    List<Long> sortedTokens = Lists.newArrayList();
    for (String tok : tokensToMasterNodes.keySet()) {
      sortedTokens.add(Long.parseLong(tok));
    }
    Collections.sort(sortedTokens);
    LOG.info(String.format("Found %d total tokens", sortedTokens.size()));
    LOG.info(String.format("Minimum tokens is %s", sortedTokens.get(0)));
    LOG.info(String.format("Maximum tokens is %s", sortedTokens.get(sortedTokens.size() - 1)));

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
      String startToken = sortedTokens.get(tokenIndex).toString();
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
    return new HashSet<Subsplit>(subsplits);
  }

  private Map<String, String> getTokenToMasterNodeMapping(Configuration conf) {

    // Create a session with a custom load-balancing policy that will ensure that we send queries
    // for system.local and system.peers to the same node.
    Cluster cluster = Cluster
        .builder()
        .addContactPoint(ConfigHelper.getInputInitialAddress(conf))
        .withPort(ConfigHelper.getInputNativeTransportPort(conf))
        .withLoadBalancingPolicy(new ConsistentHostOrderPolicy())
        .build();
    Session session = cluster.connect();

    Map<String, String> tokensToMasterNodes = Maps.newHashMap();

    // TODO: Verify that the cluster uses Murmur3Partitioner?.

    // Get the set of tokens for the local host.
    updateTokenListForLocalHost(session, tokensToMasterNodes);

    // Query the `local` and `peers` tables to get mappings from tokens to hosts.
    updateTokenListForPeers(session, tokensToMasterNodes);

    cluster.close();

    return tokensToMasterNodes;
  }

  private Session createSession(Configuration conf) {
    Cluster cluster = Cluster
        .builder()
        .addContactPoint(ConfigHelper.getInputInitialAddress(conf))
        .withPort(ConfigHelper.getInputNativeTransportPort(conf))
        .build();
    Session session = cluster.connect();
    return session;
  }

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
