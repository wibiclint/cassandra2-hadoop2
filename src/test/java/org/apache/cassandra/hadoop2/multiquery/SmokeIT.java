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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic integration smoke test.
 *
 * Write a whole bunch of data into Cassandra, then read it back.  Make sure that there aren't any
 * duplicates or anything else weird.
 *
 * We want to test the ability of the input format to group data by just the columns in the
 * partition key, or by some combination of the partitioning columns and the clustering columns.
 *
 * So our table layout looks like the following:
 *
 * key1 - int, partitioning key
 * key2 - int, partitioning key
 * key3 - string, clustering column
 * bar - int
 *
 */
public class SmokeIT {
  private static final Logger LOG = LoggerFactory.getLogger(SmokeIT.class);

  // TODO: Somehow get these from the Cassandra Maven plugin.
  private static final String HOSTIP = "127.0.0.1";
  private static final int NATIVE_PORT = 9042;

  private static final String KEYSPACE = "smoke";
  private static final String TABLE = "smoke";

  private static final String COL_PKEY1 = "key1";
  private static final String COL_PKEY2 = "key2";
  private static final String COL_CKEY3 = "key3";
  private static final String COL_VAL = "bar";

  private static final int SEED = 2014;

  // Max number of characters for the clustering column.
  private static final int CLUSTERING_COLUMN_LENGTH = 3;

  private static final int NUM_VALUES = 100000;

  /**
   * Keep track of all of the data that we have written to the table so we can make sure that we
   * read it all back.
   */
  private Map<Integer, Map<Integer, Map<String, Integer>>> mDataInTable;

  private Session mSession;

  private void createNewTable() {
    mSession.execute(String.format(
        "CREATE KEYSPACE IF NOT EXISTS %s " +
            "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };",
        KEYSPACE
    ));

    mSession.execute(String.format(
        "CREATE TABLE %s.%s (%s INT, %s INT, %s TEXT, %s INT, PRIMARY KEY ((%s, %s), %s))",
        KEYSPACE,
        TABLE,
        COL_PKEY1,
        COL_PKEY2,
        COL_CKEY3,
        COL_VAL,
        COL_PKEY1,
        COL_PKEY2,
        COL_CKEY3));
  }

  private void createLotsOfData() {
    Random random = new Random(SEED);
    mDataInTable = Maps.newHashMap();
    int numUniqueRows = 0;
    while (numUniqueRows < NUM_VALUES) {
      // The partitioning keys.
      int pkey1 = random.nextInt();
      int pkey2 = random.nextInt();

      // The clustering column.
      String ckey3 = RandomStringUtils.random(CLUSTERING_COLUMN_LENGTH);

      // The value.
      int val = random.nextInt();

      // Possibly initialize the nested maps...
      if (null == mDataInTable.get(pkey1)) {
        mDataInTable.put(pkey1, Maps.<Integer, Map<String, Integer>>newHashMap());
      }
      Map<Integer, Map<String, Integer>> mapKey2 = mDataInTable.get(pkey1);

      if (null == mapKey2.get(pkey2)) {
        mapKey2.put(pkey2, Maps.<String, Integer>newHashMap());
      }
      Map<String, Integer> mapKey3 = mapKey2.get(pkey2);

      if (!mapKey3.containsKey(ckey3)) {
        mapKey3.put(ckey3, val);
        numUniqueRows++;
      }
    }
  }

  private void insertDataIntoTable() {
    PreparedStatement insertStatement = mSession.prepare(String.format(
        "INSERT INTO %s.%s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
        KEYSPACE,
        TABLE,
        COL_PKEY1,
        COL_PKEY2,
        COL_CKEY3,
        COL_VAL
    ));
    for (Integer pkey1 : mDataInTable.keySet()) {
      Map<Integer, Map<String, Integer>> mapKey2 = mDataInTable.get(pkey1);
      for (Integer pkey2 : mapKey2.keySet()) {
        Map<String, Integer> mapKey3 = mapKey2.get(pkey2);
        for (String ckey3 : mapKey3.keySet()) {
          Integer val = mapKey3.get(ckey3);
          mSession.execute(insertStatement.bind(pkey1, pkey2, ckey3, val));
        }
      }
    }
  }

  private void connectToCassandra() {
    Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    mSession = cluster.connect();
  }

  @Test
  public void testInputFormat() {
    connectToCassandra();
    Configuration conf = new Configuration();
    ConfigHelper.setInputNativeTransportContactPoints(conf, HOSTIP);
    ConfigHelper.setInputNativeTransportPort(conf, NATIVE_PORT);

    // Create a new table and populate it with lots and lots of data.
    createNewTable();
    createLotsOfData();
    insertDataIntoTable();

    // Simple query that reads all of the columns, groups by primary key.
    ConfigHelper.setInputCqlQuery(
        conf,
        CqlQuerySpec.builder()
            .withKeyspace(KEYSPACE)
            .withTable(TABLE)
            .build()
    );

    MultiQueryCqlInputFormat inputFormat = new MultiQueryCqlInputFormat();

    // Keep track of how many times we see a given partitioning key - should happen only once!
    Set<Pair<Integer, Integer>> seenPartitionKeys = Sets.newHashSet();

    try {
      List<InputSplit> inputSplits = inputFormat.getSplitsFromConf(conf);
      MultiQueryRecordReader recordReader = new MultiQueryRecordReader();

      int numRows = 0;

      for (InputSplit inputSplit : inputSplits) {
        recordReader.initializeWithConf(inputSplit, conf);

        while (recordReader.nextKeyValue()) {
          List<Row> rows = recordReader.getCurrentValue();
          Integer pkey1 = rows.get(0).getInt(COL_PKEY1);
          Integer pkey2 = rows.get(0).getInt(COL_PKEY2);

          Pair<Integer, Integer> partitionKey = Pair.of(pkey1, pkey2);
          assertFalse(seenPartitionKeys.contains(partitionKey));
          seenPartitionKeys.add(partitionKey);

          //String ckey3 = rows.get(0).getString(COL_CKEY3);
          //assertEquals(1, rows.size());
          //int val = rows.get(0).getInt(COL_VAL);
          numRows++;
        }
      }
      assertEquals(NUM_VALUES, numRows);
    } catch (IOException ioe) {
      throw new AssertionError();
    }
  }
}
