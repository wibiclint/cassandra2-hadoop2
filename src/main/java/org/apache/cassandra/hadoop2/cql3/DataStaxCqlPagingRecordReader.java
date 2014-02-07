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

import com.datastax.driver.core.*;


import com.datastax.driver.core.exceptions.*;
import com.google.common.base.Joiner;
import org.apache.cassandra.hadoop2.ColumnFamilySplit;
import org.apache.cassandra.hadoop2.ConfigHelper;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

/*
  Note: A lot of the code below uses "token" functions in CQL to limit queries such that they hit
  only a single token range (i.e., that they go to only a particular set of replica nodes).

  You can pay around with the "token" function in the CQLSH.  See this example below:

  cqlsh:fiddle> DESCRIBE TABLE weather;

  CREATE TABLE weather (
    state text,
    city text,
    temp int,
    PRIMARY KEY ((state, city))
  ) WITH
    bloom_filter_fp_chance=0.010000 AND
    caching='KEYS_ONLY' AND
    comment='' AND
    dclocal_read_repair_chance=0.000000 AND
    gc_grace_seconds=864000 AND
    index_interval=128 AND
    read_repair_chance=0.100000 AND
    replicate_on_write='true' AND
    populate_io_cache_on_flush='false' AND
    default_time_to_live=0 AND
    speculative_retry='99.0PERCENTILE' AND
    memtable_flush_period_in_ms=0 AND
    compaction={'class': 'SizeTieredCompactionStrategy'} AND
    compression={'sstable_compression': 'LZ4Compressor'};

  cqlsh:fiddle> SELECT * FROM weather;

   state | city     | temp
  -------+----------+------
      CA |       SF |   50
      MD | Bethesda |   20

  (2 rows)

  cqlsh:fiddle> SELECT TOKEN(state, city), state, city FROM weather;

   token(state, city)   | state | city
  ----------------------+-------+----------
   -1403708236372916509 |    CA |       SF
    1447693064552575148 |    MD | Bethesda

  (2 rows)

 */

/**
 * Hadoop RecordReader to read values returned from a CQL query for processing in Hadoop.
 *
 * This class leverages the DataStax Cassandra Java driver's automatic-paging capabilities to
 * simplify its code.
 *
 * Because the Java driver returns an `Iterator<Row>` from any query, we use `Row` as our value type
 * and do not use the key type (yet).  We could eventually extract the primary key out of the `Row`
 * and make that the Mapper key.
 *
 */
public class DataStaxCqlPagingRecordReader extends RecordReader<Text, Row> {
  private static final Logger LOG = LoggerFactory.getLogger(DataStaxCqlPagingRecordReader.class);

  /** TODO: Find a reasonable value for this. */
  public static final int DEFAULT_CQL_PAGE_LIMIT = 1000;

  /** Information (start and end tokens, total number of rows) for this split. */
  private ColumnFamilySplit mColumnFamilySplit;

  /** Iterator over all of the rows for this split.  Handles paging transparently. */
  private Iterator<Row> mRowIterator;

  /** Current row. */
  private Row mCurrentRow;

  /** Open session for queries. */
  private Session mSession;

  /** Count of total number of rows returned so far.  Used for progress reporting. */
  private int mRowCount;

  /** {@inheritDoc} */
  public DataStaxCqlPagingRecordReader() { super(); }

  /** {@inheritDoc} */
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    // Get the partition key token range for this input split.
    mColumnFamilySplit = (ColumnFamilySplit) split;

    Configuration conf = context.getConfiguration();

    // Get the keyspace, column family (CQL "table"), and the columns that the user is interested in.
    String columnFamilyName = ConfigHelper.getInputColumnFamily(conf);
    String keyspace = ConfigHelper.getInputKeyspace(conf);
    String userRequestedColumns = CqlConfigHelper.getInputcolumns(conf);

    // The user can add "WHERE" clauses to the query (we shall also add a "WHERE" clause later to
    // limit the partition key ranges such that we hit onl a single set of replica nodes).
    String userDefinedWhereClauses = CqlConfigHelper.getInputWhereClauses(conf);

    // The user can also specify a consistency level for the reads for this Hadoop job.
    ConsistencyLevel consistencyLevel = ConsistencyLevel.valueOf(ConfigHelper.getReadConsistencyLevel(conf));

    // Get a connection to one of the data nodes for this input split.
    String backupHost = ConfigHelper.getInputInitialAddress(conf);
    int port = ConfigHelper.getInputNativeTransportPort(conf);
    mSession = openSession(
        mColumnFamilySplit.getLocations(),
        backupHost,
        port);

    // TODO: Pick a better exception type here.
    if (null == mSession) {
      throw new RuntimeException("Could not connect to any hosts!");
    }

    // Actually execute the query to create the row iterator!
    ResultSet resultSet = composeAndExecuteQuery(
        mSession,
        keyspace,
        columnFamilyName,
        userRequestedColumns,
        userDefinedWhereClauses,
        mColumnFamilySplit.getStartToken(),
        mColumnFamilySplit.getEndToken()
    );

    mCurrentRow = null;
    mRowCount = 0;
    mRowIterator = resultSet.iterator();

    // TODO: Fetch the first row?
    //this.nextKeyValue();
  }

  /**
   * Create an open session for reading data for this input split.
   *
   * Preferably connect to one of the replica nodes for the token range for this input split.  If
   * none are available, fall back to the original coordinator node supplied by the user.
   *
   * @param dataNodes List of replica nodes for the token range for this input split.
   * @param coordinatorNode User-specified coordinator node to use as a backup.
   * @param port RPC port for Cassandra connections.
   * @return An open session to one of the nodes.
   */
  private Session openSession(String[] dataNodes, String coordinatorNode, int port) {
    // Add all of the data nodes for this input split.
    List<String> allHostsToTry = Arrays.asList(dataNodes);

    // As a backup, try the original coordinator node specified by the user.
    //allHostsToTry.add(coordinatorNode);

    for (String host: allHostsToTry) {
      LOG.info(">>>>>>>>>>>>>> Trying to connect to host " + host + " with port " + port);
      try {
        Cluster cluster = Cluster.builder().addContactPoint(host).withPort(port).build();
        //Cluster cluster = Cluster.builder().addContactPoint("localhost").withPort(9042).build();
        return cluster.connect();
      } catch (NoHostAvailableException e) {
        Map<InetAddress, Throwable> errors = e.getErrors();
        for (InetAddress addr : errors.keySet()) {
          LOG.info(">>>> Problem with address " + addr);
          LOG.info(">>>> Problem was " + errors.get(addr).toString());
          LOG.info(">>> Trace = " + errors.get(addr).getStackTrace());
        }
        LOG.info(">>>>> " + e.toString());
        continue;
      } catch (com.datastax.driver.core.exceptions.AuthenticationException e) {
        LOG.info(e.toString());
        continue;
      }
    }
    return null;
  }

  /**
   * Create a Cassandra CQL query for the specified Hadoop job and execute it, returning a result set.
   *
   * The method will extract a list of the partition key columns from the `TableMetadata` for the
   * table being queries and use those columns, along with the start and end tokens, to limit the
   * query such that it touches only rows on a single replica node for this session.
   *
   * @param session Open session, most likely to one of the replica nodes for this split.
   * @param keyspace The C* keyspace.
   * @param table The C* table (column family) to query.
   * @param userRequestColumnsCommaSeparatedList The list of columns that the user would like to get
   *                                             back (can be null, which means fetch all).
   * @param userDefinedWhereClauses The list of filtering WHERE clauses that the users has specified
   *                                (Can be null).
   * @param startToken The partition key start token for this split.
   * @param endToken The partition key end token for this split.
   * @return The ResultSet containing the results of the query.
   */
  private ResultSet composeAndExecuteQuery(
      Session session,
      String keyspace,
      String table,
      String userRequestColumnsCommaSeparatedList,
      String userDefinedWhereClauses,
      String startToken,
      String endToken
  ) {

    session.execute(String.format("USE \"%s\";", keyspace));

    // The query we shall construct has the following components:
    // - The columns that the user has requested.
    // - Any "WHERE" clauses that the user has added.
    // - An additional "WHERE" clause that limits the partition key range to only this input split.

    // Select the columns that the user will be able to access in the Mapper.
    // If the user did not specify any columns, then select everything.
    // TODO: Shall we include the partition key columns automatically?
    String userColumnsOrAll = (null == userRequestColumnsCommaSeparatedList)
        ? "*"
        : userRequestColumnsCommaSeparatedList;

    // Add additional WHERE clauses to filter the incoming data.
    String userWhereOrBlank = (null == userDefinedWhereClauses)
        ? ""
        : " AND " + userDefinedWhereClauses;

    // Get a comma-separated list of the partition keys.
    String partitionKeyList = getPartitionKeyCommaSeparatedList(session, keyspace, table);

    String query = String.format(
        "SELECT %s FROM \"%s\" WHERE token(%s) > ? AND token(%s) <= ? %s ALLOW FILTERING;",
        userColumnsOrAll,
        table,
        partitionKeyList,
        partitionKeyList,
        userWhereOrBlank
    );

    // Bind the token limits to this query and execute!
    PreparedStatement preparedStatement = session.prepare(query);
    return session.execute(preparedStatement.bind(Long.parseLong(startToken), Long.parseLong(endToken)));
  }

  /**
   * Return a comma-separated list of the columns forming the partition key for this table.
   *
   * @param session Open session, most likely to one of the replica nodes for this split.
   * @param keyspace The C* keyspace.
   * @param table The C* table (column family) to query.
   * @return A comma-separated list (as a String) of the columns forming the partition key.
   */
  private String getPartitionKeyCommaSeparatedList(Session session, String keyspace, String table) {
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

  /** {@inheritDoc} */
  public void close() {
    mSession.shutdown();
  }

  /** {@inheritDoc} */
  public Text getCurrentKey() { return new Text("foo"); }

  /** {@inheritDoc} */
  public Row getCurrentValue() { return mCurrentRow; }

  /** {@inheritDoc} */
  public float getProgress() {
    if (null == mCurrentRow) {
      return 0.0f;
    } else {
      return ((float) mRowCount) / mColumnFamilySplit.getLength();
    }
  }

  /** {@inheritDoc} */
  public boolean nextKeyValue() throws IOException {
    if (mRowIterator.hasNext()) {
      mCurrentRow = mRowIterator.next();
      mRowCount += 1;
      return true;
    } else {
      return false;
    }
  }
}
