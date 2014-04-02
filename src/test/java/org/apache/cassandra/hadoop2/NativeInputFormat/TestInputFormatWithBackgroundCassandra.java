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
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop2.ConfigHelper;
import org.apache.cassandra.hadoop2.cql3.DataStaxCqlPagingInputFormat;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
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
