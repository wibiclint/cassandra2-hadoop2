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
package org.apache.cassandra.hadoop2.NativeInputFormat;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Cluster;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.hadoop2.ConfigHelper;
import org.apache.cassandra.hadoop2.cql3.DataStaxCqlPagingRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 *
 * TODO: Update comments.
 *
 * This version of the input format does not use any thrift functionality at all.
 *
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
public class NewCqlInputFormat extends InputFormat<Text, Row> {
  private static final Logger LOG = LoggerFactory.getLogger(NewCqlInputFormat.class);

  private String mKeyspace;
  private String mColumnFamily;
  private IPartitioner mPartitioner;

  /**
   * Validate that all of necessary configuration settings are present.
   *
   * @param conf Hadoop configuration.
   */
  protected void validateConfiguration(Cluster cluster, Configuration conf) {
    if (ConfigHelper.getInputKeyspace(conf) == null || ConfigHelper.getInputColumnFamily(conf) == null) {
      throw new UnsupportedOperationException("You must specify an input keyspace and column family.");
    }

    if (ConfigHelper.getInputInitialAddress(conf) == null) {
      throw new UnsupportedOperationException("You must specify an address for a Cassandra node.");
    }

    if (ConfigHelper.getInputPartitioner(conf) == null) {
      throw new UnsupportedOperationException("You must specify the Cassandra partitioner class.");
    }

    Metadata metadata = cluster.getMetadata();

    String inputKeyspace = ConfigHelper.getInputKeyspace(conf);
    if (null == metadata.getKeyspace(inputKeyspace)) {
      throw new UnsupportedOperationException(String.format(
          "Input keyspace '%s' does not exist.", inputKeyspace
      ));
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RecordReader<Text, Row> createRecordReader(
      InputSplit inputSplit,
      TaskAttemptContext context) {
    return new DataStaxCqlPagingRecordReader();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    Configuration conf = context.getConfiguration();
    return getSplitsFromConf(conf);
  }

  /**
   * Internal method that we can call with just a Hadoop Configuration - useful for unit testing.
   *
   * @param conf Hadoop configuration.
   * @return A list of input splits for this configuration.
   * @throws java.io.IOException
   */
  public List<InputSplit> getSplitsFromConf(Configuration conf) throws IOException {
    // Make sure that the keyspace exists.
    //LOG.info("Opening native transport connection with port " + ConfigHelper.getInputNativeTransportPort(conf));
    final Cluster cluster = Cluster
        .builder()
        .addContactPoint(ConfigHelper.getInputInitialAddress(conf))
        .withPort(ConfigHelper.getInputNativeTransportPort(conf))
        .build();
    validateConfiguration(cluster, conf);

    final Session session = cluster.newSession();

    // Get a list of all of the subsplitsFromTokens.  A "subsplit" contains the following:
    // - A token range (corresponding to a virtual node in the C* cluster).
    // - A list of replica nodes for that token range
    // - An estimated row count for that token range
    final SubsplitCreator subsplitCreator = new SubsplitCreator(session, conf);
    final Set<Subsplit> subsplitsFromTokens = subsplitCreator.createSubsplits();
    LOG.debug(String.format("Created %d subsplits from tokens", subsplitsFromTokens.size()));

    // For each range, estimate that number of rows in the table.

    // Combine or divide these ranges such that we end up with a collection of splits that are as
    // close as possible to the length that the user requested.
    final long splitSizeRequestedByUser = ConfigHelper.getInputSplitSize(conf);
    final Set<Subsplit> dividedSubsplits = divideSubsplits(subsplitsFromTokens, splitSizeRequestedByUser);
    final Set<CqlInputSplit> mergedInputSplits = combineSubsplits(dividedSubsplits, splitSizeRequestedByUser);
    LOG.debug(String.format("Created %d input splits from subsplits", mergedInputSplits.size()));

    cluster.close();

    // Get a list of all of the token ranges in the Cassandra cluster.
    List<InputSplit> inputSplitList = Lists.newArrayList();
    inputSplitList.addAll(mergedInputSplits);
    return inputSplitList;
  }

  /**
   * Break up any split that is bigger than the target split size.
   *
   * @param originalSubsplits Splits before breaking up.
   * @param maxSplitSize Split size above which we break them up.
   * @return Subsplits that are near or below the target.
   */
  private Set<Subsplit> divideSubsplits(
      Set<Subsplit> originalSubsplits,
      long maxSplitSize
  ) {
    Set<Subsplit> dividedSplits = Sets.newHashSet();

    for (Subsplit subsplit : originalSubsplits) {
      long estimatedRowCount = subsplit.getEstimatedNumberOfRows();
      // See how much bigger this is than the target.
      long timesBiggerThanTarget = estimatedRowCount / maxSplitSize;

      // Not much bigger, don't split it up.
      if (timesBiggerThanTarget <= 1) {
        dividedSplits.add(subsplit);
        continue;
      }

      // Bigger, split it up.
      Set<Subsplit> smallerSubsplits = breakupSubsplit(subsplit, timesBiggerThanTarget);
      dividedSplits.addAll(smallerSubsplits);
   }
    return dividedSplits;
  }

  private Set<Subsplit> breakupSubsplit(Subsplit bigSubsplit, long timesBiggerThanTarget) {
    final long minToken = Long.parseLong(bigSubsplit.getStartToken());
    final long maxToken = Long.parseLong(bigSubsplit.getEndToken());
    final long tokenRange = maxToken - minToken;
    final long tokenRangePerSubsplit = tokenRange / timesBiggerThanTarget;

    Set<Subsplit> smallerSubsplits = Sets.newHashSet();

    long startToken = minToken;
    do {
      long endToken = Math.min(startToken + tokenRangePerSubsplit, maxToken);
      smallerSubsplits.add(Subsplit.createFromHostSet(
          Long.toString(startToken),
          Long.toString(endToken),
          bigSubsplit.getHosts()
      ));
      startToken += tokenRangePerSubsplit;
    } while (startToken < maxToken);

    return smallerSubsplits;
  }

  /**
   * Return a comma-separated list of the columns forming the partition key for this table.
   *
   * @param session Open session, most likely to one of the replica nodes for this split.
   * @param keyspace The C* keyspace.
   * @param table The C* table (column family) to query.
   * @return A comma-separated list (as a String) of the columns forming the partition key.
   */
  public static String getPartitionKeyCommaSeparatedList(Session session, String keyspace, String table) {
    TableMetadata tableMetadata = session
        .getCluster()
        .getMetadata()
        .getKeyspace(keyspace)
        .getTable(table);
    List<ColumnMetadata> partitionKeyColumns = tableMetadata.getPartitionKey();
    List<String> columnList = new ArrayList<String>();
    for (ColumnMetadata columnMetadata : partitionKeyColumns) {
      columnList.add(columnMetadata.getName());
    }
    return Joiner.on(",").join(columnList);
  }

  /**
   * Combine splits together to try to get them as close as possible to the target size.  Combine
   * only split that share the same set of hosts.
   *
   * @param smallSubsplits
   * @param targetSplitSize
   * @return
   */
  private Set<CqlInputSplit> combineSubsplits(
      Set<Subsplit> smallSubsplits,
      long targetSplitSize) {

    // Group by host.  Should be sufficient to just grab the first host alphabetically.
    Map<String, Set<Subsplit>> subsplitsByHost = Maps.newHashMap();
    for (Subsplit subsplit : smallSubsplits) {
      String key = subsplit.getSortedHostListAsString();
      if (!subsplitsByHost.containsKey(key)) {
        subsplitsByHost.put(key, new HashSet<Subsplit>());
      }
      subsplitsByHost.get(key).add(subsplit);
    }

    // Bigger subsplits formed by combining smaller ones.
    Set<CqlInputSplit> inputSplits = Sets.newHashSet();

    // Combine splits that share hosts.
    for (Map.Entry<String, Set<Subsplit>> hostsAndSubsplits : subsplitsByHost.entrySet()) {
      inputSplits.addAll(
          combineSubsplitsSameHost(hostsAndSubsplits.getValue(), targetSplitSize)
      );
    }

    return inputSplits;
  }

  private Set<CqlInputSplit> combineSubsplitsSameHost(
      Set<Subsplit> smallSubsplits,
      long targetSplitSize) {
    // This is by no means an optimal way to do this, but it should be good enough for most cases.
    Set<CqlInputSplit> combinedInputSplits = Sets.newHashSet();
    List<Subsplit> uncombinedSmallSubsplits = Lists.newArrayList(smallSubsplits);

    // Keep popping uncombined splits off of the list of remaining splits and combining them
    // until you exceed the threshold for input split size.  When that happens, start a new input
    // split.

    int subsplitIndex = 0;
    while (subsplitIndex < uncombinedSmallSubsplits.size()) {
      Set<Subsplit> subsplitsToCombine = Sets.newHashSet();
      long totalSizeOfSubsplitsToCombine = 0L;

      while (totalSizeOfSubsplitsToCombine < targetSplitSize) {
        if (subsplitIndex >= uncombinedSmallSubsplits.size()) {
          break;
        }
        Subsplit subsplitToAdd = uncombinedSmallSubsplits.get(subsplitIndex);
        long additionalSize = subsplitToAdd.getEstimatedNumberOfRows();

        long newSize = totalSizeOfSubsplitsToCombine + additionalSize;

        if (newSize < totalSizeOfSubsplitsToCombine ||
            newSizeCloserThanOldSize(newSize, totalSizeOfSubsplitsToCombine, targetSplitSize)) {
          subsplitsToCombine.add(subsplitToAdd);
          totalSizeOfSubsplitsToCombine = newSize;
          subsplitIndex++;
        }
      }

      assert(subsplitsToCombine.size() > 0);

      // Now create the input split.
      CqlInputSplit inputSplit = CqlInputSplit.createFromSubplits(subsplitsToCombine);
      combinedInputSplits.add(inputSplit);
    }
    return combinedInputSplits;
  }

  private boolean newSizeCloserThanOldSize(long newSize, long oldSize, long targetSize) {
    return Math.abs(newSize - targetSize) <= Math.abs(oldSize - targetSize);
  }
}

