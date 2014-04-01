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

 * Test that the input format works, assuming you have a single-node Cassandra cluster running on
 * the local machine in the background.
 */
public class TestInputFormatWithBackgroundCassandra {
  private static final Logger LOG = LoggerFactory.getLogger(TestInputFormatWithBackgroundCassandra.class);
  //private static final String HOSTIP = "127.0.0.1";
  private static final String HOSTIP = "192.168.200.11";
  private static final int NATIVE_PORT = 9042;

  private static final String KEYSPACE = "keyspace_unit";
  private static final String TABLE = "table_unit";

  /**
   * Put some data into the Cassandra cluster to get started.
   */
  @BeforeClass
  public static void seedClusterWithData() {
    final Cluster cluster = Cluster.builder().addContactPoint(HOSTIP).withPort(NATIVE_PORT).build();
    final Session session = cluster.connect();
    Assert.assertNotNull(session);

    //final String deleteQuery = String.format("DROP KEYSPACE %s", KEYSPACE);
    //session.execute(deleteQuery);

    // Create a keyspace.
    final String keyspaceQuery = String.format(
        "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = " +
            " { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };", KEYSPACE);

    session.execute(keyspaceQuery);
    Assert.assertNotNull(cluster.getMetadata().getKeyspace(KEYSPACE));

    // Create a table.
    final String tableQuery = String.format(
        "CREATE TABLE IF NOT EXISTS %s.%s (name text, age int, PRIMARY KEY (name));",
        KEYSPACE,
        TABLE
    );
    session.execute(tableQuery);
    Assert.assertNotNull(cluster.getMetadata().getKeyspace(KEYSPACE).getTable(TABLE));

    // Seed the table with some data.
    List<ImmutablePair<String, Integer>> namesAndAges = Arrays.asList(
        ImmutablePair.of("Clint", 36),
        ImmutablePair.of("Jane", 35)
    );

    String insertQuery = String.format(
        "INSERT INTO %s.%s (name, age) VALUES (?, ?);",
        KEYSPACE,
        TABLE
    );
    PreparedStatement insertStatement = session.prepare(insertQuery);

    for (ImmutablePair<String, Integer> nameAndAge : namesAndAges) {
      String name = nameAndAge.getLeft();
      int age = nameAndAge.getRight();
      session.execute(insertStatement.bind(name, age));
    }
    cluster.close();
  }

  @Test
  public void testCreateSubsplits() throws IOException {
    ////////////////////////////////////////////////////////////////////////////
    // Set Cassandra / Hadoop input options.
    Configuration conf = new Configuration();
    ConfigHelper.setInputRpcPort(conf, "9160");
    ConfigHelper.setInputNativeTransportPort(conf, new Integer(NATIVE_PORT).toString());
    ConfigHelper.setInputInitialAddress(conf, HOSTIP);
    ConfigHelper.setInputColumnFamily(conf, KEYSPACE, TABLE);
    ConfigHelper.setInputPartitioner(conf, "Murmur3Partitioner");
    // The page size should be irrelevant here, since we are putting an entire document into a
    // single "text" column (shouldn't be that many total rows).
    CqlConfigHelper.setInputCQLPageRowSize(conf, "10000");
    // Keep the total number of mappers tiny --- this is just a trivial example application!
    ConfigHelper.setInputSplitSize(conf, 16*1024*1024);

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
}
