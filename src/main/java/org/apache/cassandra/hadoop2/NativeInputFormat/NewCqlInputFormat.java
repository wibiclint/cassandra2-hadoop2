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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

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
public class NewCqlInputFormat extends InputFormat<Text, Row> {
  private static final Logger LOG = LoggerFactory.getLogger(NewCqlInputFormat.class);

  /**
   * Validate that all of necessary configuration settings are present.
   *
   * @param conf Hadoop configuration.
   */
  protected void validateConfiguration(Configuration conf) {
    Preconditions.checkNotNull(NewCqlConfigHelper.getInputCqlQuery(conf));
    // TODO: Check keyspace exists before getting input splits?
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
    validateConfiguration(conf);

    // Get a list of all of the subsplitsFromTokens.  A "subsplit" contains the following:
    // - A token range (corresponding to a virtual node in the C* cluster).
    // - A list of replica nodes for that token range
    // - An estimated row count for that token range
    final SubsplitCreator subsplitCreator = new SubsplitCreator(conf);
    final Set<Subsplit> subsplitsFromTokens = subsplitCreator.createSubsplits();
    LOG.debug(String.format("Created %d subsplits from tokens", subsplitsFromTokens.size()));

    // Combine subsplits until we are close the user's requested total number of input splits.



    // Get a list of all of the token ranges in the Cassandra cluster.
    /*
    List<InputSplit> inputSplitList = Lists.newArrayList();
    inputSplitList.addAll(mergedInputSplits);
    return inputSplitList;
    */
    return null;
  }

}

