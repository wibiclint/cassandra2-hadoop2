package org.apache.cassandra.hadoop2.multiquery;
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


import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

// THIS IS AN INTEGRATION TESTS AND REQUIRES A LIVE CASSANDRA CLUSTER TO CONNECT TO!

/**
 * Test assuming that you have some kind of local C* server running (could be single-node local,
 * could be multi-node on a Vagrant machine, etc.
 *
 * Use the include Python script to seed the table with data.
 */
public class TestInputFormatWithBackgroundCassandra {
  private static final Logger LOG = LoggerFactory.getLogger(TestInputFormatWithBackgroundCassandra.class);
  private static final String HOSTIP = "192.168.200.11";
  private static final int NATIVE_PORT = 9042;

  private static final String KEYSPACE = "test_data";
  private static final String TABLE = "users";
/*
  @Test
  public void testCreateSubsplits() throws IOException {
    // Set Cassandra / Hadoop input options.
    Configuration conf = new Configuration();
    ConfigHelper.setInputNativeTransportPort(conf, NATIVE_PORT);
    ConfigHelper.setInputNativeTransportContactPoints(conf, HOSTIP);

    MultiQueryCqlInputFormat inputFormat = new MultiQueryCqlInputFormat();

    Session session = null;

    SubsplitCreator subsplitCreator = new SubsplitCreator(conf, session);
    List<Subsplit> subsplits = subsplitCreator.createSubsplits();

    LOG.info(String.format("Got back %d subsplits", subsplits.size()));
    //for (Subsplit subsplit: subsplits) {
      //LOG.info(subsplit.toString());
    //}
  }
  */


  @Test
  public void testCombineSubsplits() throws IOException {
    final String HOST0 = "host0";
    final String HOST1 = "host1";
    List<Subsplit> smallSubsplits = Lists.newArrayList(
        Subsplit.createFromHost("0", "1", HOST0),
        Subsplit.createFromHost("2", "3", HOST1),
        Subsplit.createFromHost("4", "5", HOST0),
        Subsplit.createFromHost("6", "7", HOST1)
    );

    Configuration conf = new Configuration();
    ConfigHelper.setInputTargetNumSplits(conf, 2);

    SubsplitCombiner subsplitCombiner = new SubsplitCombiner(conf);

    List<MultiQueryInputSplit> inputSplits = subsplitCombiner.combineSubsplits(smallSubsplits);

    Assert.assertEquals(2, inputSplits.size());
  }

  // TODO: Add check that hosts are grouped appropriately.
}
