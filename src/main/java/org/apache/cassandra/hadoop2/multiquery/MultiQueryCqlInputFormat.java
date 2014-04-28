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
package org.apache.cassandra.hadoop2.multiquery;

import java.io.IOException;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Update javadoc.

/**
 * Hadoop InputFormat for Cassandra that does not use Cassandra's Thrift API.
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
public class MultiQueryCqlInputFormat extends InputFormat<Text, Row> {
  private static final Logger LOG = LoggerFactory.getLogger(MultiQueryCqlInputFormat.class);

  /**
   * Validate that all of necessary configuration settings are present.
   *
   * @param conf Hadoop configuration.
   * @param session Open C* session.
   * @throws IOException if the configuration is invalid.
   */
  protected void validateConfiguration(Configuration conf, Session session) throws IOException {
    List<CqlQuerySpec> queries = ConfigHelper.getInputCqlQueries(conf);

    if (0 == queries.size()) {
      throw new IOException("Must specify a query!");
    }

    // Check that all keyspaces and tables exist.
    MultiQueryRecordReader.checkKeyspacesAndTablesExist(session, queries);

    // Check that all tables have the same partition keys.
    MultiQueryRecordReader.checkParitionKeysAreIdentical(session, queries);

    // Check that all queried columns exist.

    // Check that all specified clustering columns are the same across tables.
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RecordReader<Text, Row> createRecordReader(
      InputSplit inputSplit,
      TaskAttemptContext context) {
    //return new DataStaxCqlPagingRecordReader();
    return null;
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
   * @return A list of input splits for a MapReduce job.
   * @throws java.io.IOException
   */
  public List<InputSplit> getSplitsFromConf(Configuration conf) throws IOException {
    // Create a session with a custom load-balancing policy that will ensure that we send queries
    // for system.local and system.peers to the same node.
    Cluster cluster = Cluster
        .builder()
        .addContactPoints(ConfigHelper.getInputNativeTransportContactPoints(conf))
        .withPort(ConfigHelper.getDefaultInputNativeTransportPort(conf))
        .withLoadBalancingPolicy(new ConsistentHostOrderPolicy())
        .build();
    Session session = cluster.connect();

    validateConfiguration(conf, session);

    // Get a list of all of the subsplits.  A "subsplit" contains the following:
    // - A token range (corresponding to a virtual node in the C* cluster)
    // - A list of replica nodes for that token range
    final SubsplitCreator subsplitCreator = new SubsplitCreator(conf, session);
    final List<Subsplit> subsplitsFromTokens = subsplitCreator.createSubsplits();
    LOG.debug(String.format("Created %d subsplits from tokens", subsplitsFromTokens.size()));

    // In this InputFormat, we allow the user to specify a desired number of InputSplits.  We
    // will likely have far more subsplits (vnodes) than desired InputSplits.  Therefore, we combine
    // subsplits (hopefully those that share the same replica nodes) until we get to our desired
    // InputSplit count.
    final SubsplitCombiner subsplitCombiner = new SubsplitCombiner(conf);

    // Get a list of all of the token ranges in the Cassandra cluster.
    List<InputSplit> inputSplitList = Lists.newArrayList();

    // Java is annoying here about casting a list.
    inputSplitList.addAll(subsplitCombiner.combineSubsplits(subsplitsFromTokens));
    cluster.close();
    return inputSplitList;
  }

}

