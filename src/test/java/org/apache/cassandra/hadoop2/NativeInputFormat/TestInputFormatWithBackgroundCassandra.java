package org.apache.cassandra.hadoop2.NativeInputFormat;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop2.ConfigHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Test assuming that you have some kind of local C* server running (could be single-node local,
 * could be multi-node on a Vagrant machine, etc.
 *
 * Use the include Python script to seed the table with data.
 */
public class TestInputFormatWithBackgroundCassandra {
  private static final Logger LOG = LoggerFactory.getLogger(TestInputFormatWithBackgroundCassandra.class);
  //private static final String HOSTIP = "127.0.0.1";
  private static final String HOSTIP = "192.168.200.11";
  private static final int NATIVE_PORT = 9042;

  private static final String KEYSPACE = "test_data";
  private static final String TABLE = "users";

  @Test
  public void testCreateSubsplits() throws IOException {
    ////////////////////////////////////////////////////////////////////////////
    // Set Cassandra / Hadoop input options.
    Configuration conf = new Configuration();
    ConfigHelper.setInputRpcPort(conf, "9160");
    ConfigHelper.setInputNativeTransportPort(conf, Integer.toString(NATIVE_PORT));
    ConfigHelper.setInputInitialAddress(conf, HOSTIP);
    ConfigHelper.setInputColumnFamily(conf, KEYSPACE, TABLE);
    ConfigHelper.setInputPartitioner(conf, "Murmur3Partitioner");

    // The page size should be irrelevant here, since we are putting an entire document into a
    // single "text" column (shouldn't be that many total rows).
    CqlConfigHelper.setInputCQLPageRowSize(conf, "10000");

    // Keep the total number of mappers tiny --- this is just a trivial example application!
    //ConfigHelper.setInputSplitSize(conf, 16*1024*1024);
    ConfigHelper.setInputSplitSize(conf, 1000);

    NewCqlInputFormat inputFormat = new NewCqlInputFormat();
    final Cluster cluster = Cluster.builder().addContactPoint(HOSTIP).withPort(NATIVE_PORT).build();
    final Session session = cluster.connect();
    SubsplitCreator subsplitCreator = new SubsplitCreator(session, conf);
    Set<Subsplit> subsplits = subsplitCreator.createSubsplits();

    LOG.info(String.format("Got back %d subsplits", subsplits.size()));
    /*
    for (Subsplit subsplit: subsplits) {
      LOG.info(subsplit.toString());
    }
    */
  }

  @Test
  public void testBreakUpSubsplit() throws IOException {
    // Create a subsplit that is larger than what the user requested.
    final long targetSize = 100L;
    final int numEventualSplits = 4;
    final long startToken = 0L;
    final long endToken = startToken + numEventualSplits * targetSize;
    final String HOST = "host";
    Subsplit bigSubsplit = Subsplit.createFromHost(
        Long.toString(startToken),
        Long.toString(endToken),
        HOST
    );
    // Keep the math easy -> assume one row per increment in the token space.
    bigSubsplit.setEstimatedNumberOfRows(endToken - startToken);

    Set<Subsplit> biggerSubsplits = Sets.newHashSet(bigSubsplit);

    NewCqlInputFormat newCqlInputFormat = new NewCqlInputFormat();

    Set<Subsplit> smallSubsplits = newCqlInputFormat.divideSubsplits(biggerSubsplits, targetSize);

    for (Subsplit subsplit : smallSubsplits) {
      LOG.debug(subsplit.toString());
      Assert.assertEquals(
          targetSize,
          Long.parseLong(subsplit.getEndToken()) - Long.parseLong(subsplit.getStartToken())
      );
    }
  }

  @Test
  public void testCombineSubsplitsUnderTarget() throws IOException {
    // Create a couple of subsplits that are smaller than the target.
    final String HOST = "host";
    List<Subsplit> smallSubsplits = Lists.newArrayList(
        // Target is 60, getting to 50 is closer than getting to 100.
        Subsplit.createFromHost("0", "1", HOST, 20),
        Subsplit.createFromHost("2", "3", HOST, 30),
        Subsplit.createFromHost("4", "5", HOST, 50)
    );

    final long targetSize = 60L;

    NewCqlInputFormat newCqlInputFormat = new NewCqlInputFormat();

    Set<CqlInputSplit> inputSplits =
        newCqlInputFormat.combineSubsplitsSameHost(smallSubsplits, targetSize);

    for (CqlInputSplit inputSplit : inputSplits) {
      LOG.debug(inputSplit.toString());
      Assert.assertEquals(50L, inputSplit.getLength());
    }
  }

  @Test
  public void testCombineSubsplitsOverTarget() throws IOException {
    // Create a couple of subsplits that are smaller than the target.
    final String HOST = "host";
    List<Subsplit> smallSubsplits = Lists.newArrayList(
        // Target is 60, getting to 70 is closer than getting to 10.
        Subsplit.createFromHost("0", "1", HOST, 10),
        Subsplit.createFromHost("2", "3", HOST, 60),
        Subsplit.createFromHost("4", "5", HOST, 70)
    );

    final long targetSize = 60L;

    NewCqlInputFormat newCqlInputFormat = new NewCqlInputFormat();

    Set<CqlInputSplit> inputSplits =
        newCqlInputFormat.combineSubsplitsSameHost(smallSubsplits, targetSize);

    for (CqlInputSplit inputSplit : inputSplits) {
      LOG.debug(inputSplit.toString());
      Assert.assertEquals(70L, inputSplit.getLength());
    }
  }

  @Test
  public void testCreateInputSplits() throws IOException {
    ////////////////////////////////////////////////////////////////////////////
    // Set Cassandra / Hadoop input options.
    Configuration conf = new Configuration();
    ConfigHelper.setInputRpcPort(conf, "9160");
    ConfigHelper.setInputNativeTransportPort(conf, Integer.toString(NATIVE_PORT));
    ConfigHelper.setInputInitialAddress(conf, HOSTIP);
    ConfigHelper.setInputColumnFamily(conf, KEYSPACE, TABLE);
    ConfigHelper.setInputPartitioner(conf, "Murmur3Partitioner");

    // The page size should be irrelevant here, since we are putting an entire document into a
    // single "text" column (shouldn't be that many total rows).
    CqlConfigHelper.setInputCQLPageRowSize(conf, "10000");

    // Keep the total number of mappers tiny --- this is just a trivial example application!
    //ConfigHelper.setInputSplitSize(conf, 16*1024*1024);
    ConfigHelper.setInputSplitSize(conf, 1000);

    NewCqlInputFormat inputFormat = new NewCqlInputFormat();
    final Cluster cluster = Cluster.builder().addContactPoint(HOSTIP).withPort(NATIVE_PORT).build();
    final Session session = cluster.connect();

    List<InputSplit> inputSplits = inputFormat.getSplitsFromConf(conf);
  }
}
