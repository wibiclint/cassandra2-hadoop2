/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.hadoop2.cql3;

import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.hadoop2.ColumnFamilySplit;
import org.apache.cassandra.hadoop2.ConfigHelper;
import org.apache.cassandra.thrift.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Hadoop InputFormat allowing map/reduce against Cassandra rows within one ColumnFamily.
 *
 * This code uses the DataStax Java driver to fetch data from tables.
 *
 * At minimum, you need to set the KS and CF in your Hadoop job Configuration.  
 * The ConfigHelper class is provided to make this simple:
 *   ConfigHelper.setInputColumnFamily
 *
 * You can also configure:
 *
 * the number of rows per InputSplit with
 *   ConfigHelper.setInputSplitSize. The default split size is 64k rows.
 *
 * the number of CQL rows per page
 *   CQLConfigHelper.setInputCQLPageRowSize. The default page row size is 1000. You
 *   should set it to "as big as possible, but no bigger." It set the LIMIT for the CQL 
 *   query, so you need set it big enough to minimize the network overhead, and also
 *   not too big to avoid out of memory issue.
 *   
 * the column names of the select CQL query. The default is all columns
 *   CQLConfigHelper.setInputColumns
 *   
 * the user defined the where clause
 *   CQLConfigHelper.setInputWhereClauses. The default is no user defined where clause
 */
public class DataStaxCqlPagingInputFormat extends InputFormat<Text, Row> {
  private static final Logger LOG = LoggerFactory.getLogger(DataStaxCqlPagingInputFormat.class);

  private String mKeyspace;
  private String mColumnFamily;
  private IPartitioner mPartitioner;

  /**
   * Validate that all of necessary configuration settings are present.
   *
   * @param conf Hadoop configuration.
   */
  protected void validateConfiguration(Configuration conf) {
    if (ConfigHelper.getInputKeyspace(conf) == null || ConfigHelper.getInputColumnFamily(conf) == null) {
      throw new UnsupportedOperationException("You must specify an input keyspace and column family.");
    }

    if (ConfigHelper.getInputInitialAddress(conf) == null) {
      throw new UnsupportedOperationException("You must specify an address for a Cassandra node.");
    }

    if (ConfigHelper.getInputPartitioner(conf) == null) {
      throw new UnsupportedOperationException("You must specify the Cassandra partitioner class.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public RecordReader<Text, Row> createRecordReader(
      InputSplit inputSplit,
      TaskAttemptContext context) {
    return new DataStaxCqlPagingRecordReader();
  }

  /** {@inheritDoc} */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    LOG.info("-------------------- Getting input splits --------------------");
    Configuration conf = context.getConfiguration();
    validateConfiguration(conf);

    // Get a list of all of the token ranges in the Cassandra cluster.
    List<TokenRange> masterRangeNodes = getRangeMap(conf);
    LOG.info("Got " + masterRangeNodes.size() + " master range nodes");

    mKeyspace = ConfigHelper.getInputKeyspace(context.getConfiguration());
    mColumnFamily = ConfigHelper.getInputColumnFamily(context.getConfiguration());

    // TODO: Can't we just get this from the Cassandra.Client?
    mPartitioner = ConfigHelper.getInputPartitioner(context.getConfiguration());

    // For each individual token range, retrieve a set of input splits.  We need to do this in case
    // the number of rows in a given token range in our ring is greater than the user-specified
    // split size => we need more than one input split per token range (per replica node).
    ExecutorService executor = Executors.newCachedThreadPool();
    List<InputSplit> splits = new ArrayList<InputSplit>();

    try {
      List<Future<List<InputSplit>>> splitFutures = new ArrayList<Future<List<InputSplit>>>();

      // Get the user-specified key range.
      // Only relevant if you are using an order-preserving hash function.
      KeyRange jobKeyRange = ConfigHelper.getInputKeyRange(conf);
      if (jobKeyRange != null) {
        throw new UnsupportedOperationException("Not currently supporting key ranges.");
      }

      for (TokenRange range : masterRangeNodes) {
        // For each range, pick a live owner (replica node) and ask it to compute input splits that
        // will be match the user's requested split size.
        splitFutures.add(executor.submit(new SplitCallable(range, conf)));
      }

      // wait until we have all the results back
      for (Future<List<InputSplit>> futureInputSplits : splitFutures) {
        try {
          splits.addAll(futureInputSplits.get());
        } catch (Exception e) {
          throw new IOException("Could not get input splits", e);
        }
      }
    } finally {
      executor.shutdownNow();
    }

    assert splits.size() > 0;
    Collections.shuffle(splits, new Random(System.nanoTime()));
    return splits;
  }

  /**
   * Get a list of token ranges for the given Cassandra cluster.
   *
   * Each `TokenRange` consists of a start and end token and a list of endpoints (replica nodes),
   * among other things.
   *
   * @param conf The Hadoop job configuration.
   * @return A list of the token ranges for this cluster.
   * @throws IOException
   */
  private List<TokenRange> getRangeMap(Configuration conf) throws IOException {
    Cassandra.Client client = ConfigHelper.getClientFromInputAddressList(conf);
    List<TokenRange> map;
    try {
      map = client.describe_ring(ConfigHelper.getInputKeyspace(conf));
    } catch (InvalidRequestException e) {
      // TODO: No idea why this would happen...
      throw new RuntimeException(e);
    } catch (TException e) {
      // Yikes!
      throw new RuntimeException(e);

    }
    return map;
  }

  /**
   * Gets a token range and splits it up according to the suggested size into input splits that
   * Hadoop can use.
   */
  class SplitCallable implements Callable<List<InputSplit>> {
    private final TokenRange range;
    private final Configuration conf;

    public SplitCallable(TokenRange tr, Configuration conf) {
      this.range = tr;
      this.conf = conf;
    }

    private String[] translateEndpointIpAddressesToHostNames(
        List<String> endpoints,
        List<String> rpcEndpoints
    ) throws UnknownHostException {
      String[] hostNames = endpoints.toArray(new String[range.endpoints.size()]);

      // hadoop needs hostname, not ip
      int endpointIndex = 0;

      for (String endpoint: rpcEndpoints) {
        String endpoint_address = endpoint;
        if (endpoint_address == null || endpoint_address.equals("0.0.0.0")) {
          endpoint_address = endpoints.get(endpointIndex);
        }
        hostNames[endpointIndex++] = InetAddress.getByName(endpoint_address).getHostName();
      }
      return hostNames;
    }

    /**
     * Given a token range, return a list of input splits.
     *
     * @return A list of input splits that should match as closely as possible the requested split
     *     size.
     * @throws Exception
     */
    public List<InputSplit> call() throws Exception {
      assert range.rpc_endpoints.size() == range.endpoints.size() : "rpc_endpoints size must match endpoints size";

      // turn the sub-ranges into InputSplits
      String[] endpoints = translateEndpointIpAddressesToHostNames(
          range.endpoints,
          range.rpc_endpoints);

      ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
      List<CfSplit> subSplits = getSubSplits(mKeyspace, mColumnFamily, range, conf);
      Token.TokenFactory factory = mPartitioner.getTokenFactory();

      // Turn all of the Thrift API CfSplit objects into ColumnFamilySplit objects.
      for (CfSplit subSplit : subSplits) {
        Token left = factory.fromString(subSplit.getStart_token());
        Token right = factory.fromString(subSplit.getEnd_token());
        Range<Token> range = new Range<Token>(left, right, mPartitioner);
        List<Range<Token>> ranges = range.isWrapAround() ? range.unwrap() : ImmutableList.of(range);

        for (Range<Token> subRange : ranges) {
          ColumnFamilySplit split = new ColumnFamilySplit(
              factory.toString(subRange.left),
              factory.toString(subRange.right),
              subSplit.getRow_count(),
              endpoints);

          LOG.debug("adding " + split);
          splits.add(split);
        }
      }
      return splits;
    }
  }

  /**
   * Given a list of split points from a client that does not support `describe_splits_ex`, return
   * a list of `CfSplit` objects.
   *
   * @param splitTokens
   * @param splitSize
   * @return
   */
  private List<CfSplit> tokenListToSplits(List<String> splitTokens, int splitSize) {
    List<CfSplit> splits = Lists.newArrayListWithExpectedSize(splitTokens.size() - 1);
    for (int j = 0; j < splitTokens.size() - 1; j++)
      splits.add(new CfSplit(splitTokens.get(j), splitTokens.get(j + 1), splitSize));
    return splits;
  }

  /**
   * For a given token range (and corresponding set of replica nodes), compute a set of splits such
   * that each split is as close as possible to the split size requested by the user.
   *
   * @param keyspace
   * @param cfName
   * @param range
   * @param conf
   * @return
   * @throws IOException
   */
  private List<CfSplit> getSubSplits(
      String keyspace,
      String cfName,
      TokenRange range,
      Configuration conf) throws IOException {
    // Get the split size that the user has requested.
    int splitSize = ConfigHelper.getInputSplitSize(conf);

    // Loop through the replicate nodes for this token range until we can connect to one.
    // Then request and the appropriate input splits and return them.
    for (int i = 0; i < range.rpc_endpoints.size(); i++) {
      String host = range.rpc_endpoints.get(i);

      if (host == null || host.equals("0.0.0.0"))
        host = range.endpoints.get(i);

      try {
        Cassandra.Client client = ConfigHelper.createConnection(
            conf,
            host,
            ConfigHelper.getInputRpcPort(conf)
        );
        client.set_keyspace(keyspace);

        // First try this more sophisticated / newer "describe splits" call to the replica node.
        try {
          return client.describe_splits_ex(cfName, range.start_token, range.end_token, splitSize);
        } catch (InvalidRequestException ire) {
          // Fallback to guessing split size if talking to server without describe_splits_ex method.
          List<String> splitPoints = client.describe_splits(
              cfName,
              range.start_token,
              range.end_token,
              splitSize
          );
          return tokenListToSplits(splitPoints, splitSize);
        }
      } catch (IOException e) {
        LOG.debug("failed connect to endpoint " + host, e);
      } catch (InvalidRequestException e) {
        throw new RuntimeException(e);
      } catch (TException e) {
        // Yikes!
        throw new RuntimeException(e);
      }
    }
    throw new IOException("failed connecting to all endpoints " + StringUtils.join(range.endpoints, ","));
  }

}
