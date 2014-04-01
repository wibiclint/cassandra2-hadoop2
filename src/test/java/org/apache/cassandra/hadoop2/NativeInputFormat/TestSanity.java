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


import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.Sets;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop2.ConfigHelper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**

 * Test that the input format works, assuming you have a single-node Cassandra cluster running on
 * the local machine in the background.
 */
public class TestSanity {
  private static final Logger LOG = LoggerFactory.getLogger(TestSanity.class);
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

    // Create a keyspace.
    final String keyspaceQuery = String.format(
        "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = " +
            " { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };", KEYSPACE
    );

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
  }

  @Test
  public void testCreateSubsplits() throws IOException {
    InetAddress fixedHostAddress = InetAddress.getByName(HOSTIP);
    Cluster cluster = Cluster.builder()
        .addContactPoint(HOSTIP)
        .withPort(NATIVE_PORT)
        //.withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
        //.withLoadBalancingPolicy(FixedHostPolicy.fromFixedHostAddress(fixedHostAddress))
        .withLoadBalancingPolicy(new ConsistentHostOrderPolicy())
            .build();
    Session session = cluster.connect();

    // Arbitrary routing key to use to ensure that the two queries about system tables go to the
    // same host.
    ByteBuffer routingKey = Bytes.fromHexString("0xDEADBEEF");
    SimpleStatement localHostStatement = new SimpleStatement("SELECT tokens from system.local");
    localHostStatement.setRoutingKey(routingKey);

    // Get local host tokens.
    Set<String> localTokens =
        session.execute(localHostStatement).one().getSet("tokens", String.class);

    // Get the tokens for the other hosts.
    SimpleStatement peersStatement = new SimpleStatement("SELECT peer, tokens from system.peers");
    peersStatement.setRoutingKey(routingKey);
    List<Row> rows = session.execute(peersStatement).all();
    assert(rows.size() == 2);

    Set<String> firstRemoteTokens = rows.get(0).getSet("tokens", String.class);
    Set<String> secondRemoteTokens = rows.get(1).getSet("tokens", String.class);

    int totalNumberOfTokens =
        localTokens.size() + firstRemoteTokens.size() + secondRemoteTokens.size();

    Set<String> distinctTokens = Sets.newHashSet();
    distinctTokens.addAll(localTokens);
    distinctTokens.addAll(firstRemoteTokens);
    distinctTokens.addAll(secondRemoteTokens);

    Assert.assertEquals(totalNumberOfTokens, distinctTokens.size());

  }
}
