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
  private final Session session;

  /** Fraction of subsplits to use for sampling estimated number of rows / subsplit. */
  private static final double SUBPLIT_SAMPLE_FRACTION = 0.1;

  public SubsplitCreator(Session session, Configuration conf) {
    this.conf = conf;
    this.session = session;
  }

  public Set<Subsplit> createSubsplits() {
    // Create subsplits that initially contain a mapping from token ranges to primary hosts.
    Set<Subsplit> subsplits = createInitialSubsplits();

    // Add replica nodes to the subsplits.

    // Estimate row counts for the subsplits.
    estimateRowCountsAndUpdateSubsplits(subsplits);

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

  /**
   * Given a set of subsplits, update them with estimate row counts.
   *
   * Query a random subset of the subsplits to get an row counts, and assume all subplits are
   * around the average.
   *
   * @param subsplits
   */
  private void estimateRowCountsAndUpdateSubsplits(Set<Subsplit> subsplits) {
    // Create the query to use for getting row counts.
    PreparedStatement rowCountStatement = buildRowCountStatement();

    // Pick a random subset of the subsplits to use.
    Set<Subsplit> sampleSet = getSubsplitSample(subsplits);

    // Get the row counts for the subset of subsplits.
    Set<Long> rowCounts = fetchSubsplitSampleRowCounts(sampleSet, rowCountStatement);

    // Calculate the average row counts.
    long averageRowCount = computeAverageRowCount(rowCounts);
    LOG.debug(String.format("Average row count is %s", averageRowCount));

    // Assign the average row count to all of the subsplits.
    for (Subsplit subsplit : subsplits) {
      subsplit.setEstimatedNumberOfRows(averageRowCount);
    }

  }

  private PreparedStatement buildRowCountStatement() {
    // TODO: Possibly share some code here with the RecordReader.
    // Get the keyspace, column family (CQL "table"), and the columns that the user is interested in.
    String table = ConfigHelper.getInputColumnFamily(conf);
    String keyspace = ConfigHelper.getInputKeyspace(conf);

    // The user can add "WHERE" clauses to the query (we shall also add a "WHERE" clause later to
    // limit the partition key ranges such that we hit onl a single set of replica nodes).
    String userDefinedWhereClauses = CqlConfigHelper.getInputWhereClauses(conf);

    // Add additional WHERE clauses to filter the incoming data.
    String userWhereOrBlank = (null == userDefinedWhereClauses)
        ? ""
        : " AND " + userDefinedWhereClauses;

    // Get a comma-separated list of the partition keys.
    String partitionKeyList = NewCqlInputFormat.getPartitionKeyCommaSeparatedList(
        session,
        keyspace,
        table);

    String query = String.format(
        "SELECT COUNT(*) FROM \"%s\".\"%s\" WHERE token(%s) > ? AND token(%s) <= ? %s ALLOW FILTERING;",
        keyspace,
        table,
        partitionKeyList,
        partitionKeyList,
        userWhereOrBlank
    );

    return session.prepare(query);
  }

  /**
   * Create a random sample of the subsplits to use for estimating row counts.
   *
   * @param allSubsplits A set of all of the subsplits for the cluster.
   * @return
   */
  private Set<Subsplit> getSubsplitSample(Set<Subsplit> allSubsplits) {
    List<Subsplit> subsplitList = new ArrayList(allSubsplits);
    Collections.shuffle(subsplitList);

    int numSubsplitsToSample = (int)(subsplitList.size() * SUBPLIT_SAMPLE_FRACTION);

    return new HashSet<Subsplit>(subsplitList.subList(0, numSubsplitsToSample));
  }

  private Set<Long> fetchSubsplitSampleRowCounts(
      Set<Subsplit> subsplitSample,
      PreparedStatement rowCountStatement) {
    Set<ResultSetFuture> futures = Sets.newHashSet();

    for (Subsplit subsplit : subsplitSample) {
      futures.add(session.executeAsync(rowCountStatement.bind(
          Long.parseLong(subsplit.getStartToken()),
          Long.parseLong(subsplit.getEndToken())
      )));
    }

    Set<Long> rowCounts = Sets.newHashSet();

    // TODO: Optimize this code to more efficiently process the futures.
    for (ResultSetFuture resultSetFuture : futures) {
      ResultSet resultSet = resultSetFuture.getUninterruptibly();
      List<Row> rows = resultSet.all();
      // Should be just one result - the row count!
      assert(rows.size() == 1);

      long count = rows.get(0).getLong(0);
      assert(count >= 0);

      rowCounts.add(count);
    }
    return rowCounts;
  }

  private long computeAverageRowCount(Set<Long> rowCounts) {
    long sum = 0;
    for (Long count : rowCounts) {
      sum += count;
    }
    return sum / rowCounts.size();
  }
}
