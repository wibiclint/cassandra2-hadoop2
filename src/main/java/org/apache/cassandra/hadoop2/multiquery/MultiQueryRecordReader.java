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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
  Note: A lot of the code below uses "token" functions in CQL to limit queries such that they hit
  only a single token range (i.e., that they go to only a particular set of replica nodes).

  You can play around with the "token" function in the CQLSH.  See this example below:

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
public class MultiQueryRecordReader extends RecordReader<Text, List<Row>> {
  private static final Logger LOG = LoggerFactory.getLogger(MultiQueryRecordReader.class);

  /** Information (start and end tokens, total number of rows) for this split. */
  private MultiQueryInputSplit mInputSplit;

  /** List of user-specified queries, with token ranges in them. */
  private List<CqlQueryWithArgsToBind> mCqlQueries;

  /**
   * Iterator over all of the rows returned for all of our various queries.
   *
   * This iterator combines all of the rows for all of our separate queries.  We need to reassign
   * this iterator every time we need to fetch a new token range.
   */
  private MultiRowIterator mMultiRowIterator = null;

  /** Current set of fresh rows, ready to serve!. */
  private List<Row> mCurrentRows;

  /** Iterator over all of the token ranges for this split. */
  private Iterator<TokenRange> mTokenRangeIterator;

  /** Open session for queries. */
  private Session mSession;

  /** Count of total number of rows returned so far.  Used for progress reporting. */
  private int mRowCount;

  private static final Joiner COMMA_JOINER = Joiner.on(", ");

  private String mTokenColumn;

  private Configuration mConf;

  /** {@inheritDoc} */
  public MultiQueryRecordReader() { super(); }


  /** {@inheritDoc} */
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    initializeWithConf(split, context.getConfiguration());
  }

  void initializeWithConf(InputSplit split, Configuration conf) throws IOException {
    // Get the partition key token range for this input split.
    Preconditions.checkArgument(split instanceof MultiQueryInputSplit);
    if (split instanceof MultiQueryInputSplit) {
      mInputSplit = (MultiQueryInputSplit) split;
    } else {
      throw new IOException("Illegal input split format " + MultiQueryInputSplit.class);
    }

    mConf = conf;
    mSession = openSession(mConf);

    // Extract the CQL query from the Configuration and make a prepared statement with
    // placeholders (?s) for the token range.
    initializeCqlStatements(mConf);

    // Create an iterator to go through all of the token ranges.
    mTokenRangeIterator = mInputSplit.getTokenRangeIterator();
    assert(mTokenRangeIterator.hasNext());

    mRowCount = 0;
    queryNextTokenRange();
  }

  private String getTokenColumn(Configuration conf) {
    List<CqlQuerySpec> cqlQuerySpecs =
        ConfigHelper.getInputCqlQueries(conf);
    if (cqlQuerySpecs.size() == 0) {
      throw new IllegalArgumentException(
          "Cannot initialize RecordReader without specifying at least one query.");
    }

    // Figure out the columns in the partition key.
    List<String> partitioningKeys = getPartitioningKeysForQuery(cqlQuerySpecs.get(0));
    return String.format("token(%s)", COMMA_JOINER.join(partitioningKeys));
  }

  /**
   * Initialize the list of CQL statements to execute user-specified queries over a set of Cassandra
   * column families.
   *
   * Create the statements with the user-specified table names, columns to fetch, and WHERE clauses.
   * Add WHERE clauses for the token range, and make sure to select the token of the primary key.
   */
  private void initializeCqlStatements(Configuration conf) {
    mTokenColumn = getTokenColumn(conf);
    // Create a CQL statement that we can use to fetch data per the user's specification.
    mCqlQueries = Lists.newArrayList();

    List<CqlQuerySpec> querySpecs = ConfigHelper.getInputCqlQueries(conf);

    for (CqlQuerySpec querySpec : querySpecs) {
      CqlQueryWithArgsToBind query = createCqlQuery(querySpec);
      LOG.debug(query.toString());
      LOG.debug("Created query:");
      LOG.debug(query.toString());
      mCqlQueries.add(query);
    }
  }

  private CqlQueryWithArgsToBind createCqlQuery(CqlQuerySpec querySpec) {
    // TODO: Check that these don't have any quotes in them?
    final String keyspace = querySpec.getKeyspace();
    final String table = querySpec.getTable();
    final WhereClause userDefinedWhereClauses = querySpec.getWhereClauses();

    // Construct a new where clause that include the token range.
    String whereTokenClause = String.format(
        "%s >= ? AND %s <= ?",
        mTokenColumn,
        mTokenColumn);
    final String whereClause;
    // Very important here that the token values are the *last* values to bind in the statement!
    if (null != userDefinedWhereClauses) {
      whereClause = userDefinedWhereClauses.getClause() + " AND " + whereTokenClause;
    } else {
      whereClause = "WHERE " + whereTokenClause;
    }

    final String columnCsv = getColumnCsvForQuery(querySpec);

    final String queryString = String.format(
        "SELECT %s from \"%s\".\"%s\" %s ALLOW FILTERING",
        columnCsv,
        keyspace,
        table,
        whereClause
    );

    PreparedStatement preparedStatement = mSession.prepare(queryString);
    return new CqlQueryWithArgsToBind(
        preparedStatement,
        userDefinedWhereClauses == null
            ? Lists.<Serializable>newArrayList()
            : userDefinedWhereClauses.getArgs()
    );
  }

  private String getColumnCsvForQuery(CqlQuerySpec querySpec) {
    final String columnCsv;

    // If the user asked for "*", then get every column in the table.
    if (querySpec.getColumnCsv().equals("*")) {
      TableMetadata tableMetadata = mSession
          .getCluster()
          .getMetadata()
          .getKeyspace(querySpec.getKeyspace())
          .getTable(querySpec.getTable());
      List<String> columns = Lists.newArrayList();
      for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
        columns.add(columnMetadata.getName());
      }
      columnCsv = COMMA_JOINER.join(columns);
    } else {
      // Add any extra columns here...
      List<String> groupingClusteringColumns =
          ConfigHelper.getInputCqlQueryClusteringColumns(mConf);

      List<String> userColumns = Splitter.on(",").splitToList(querySpec.getColumnCsv());
      List<String> allColumns = Lists.newArrayList(groupingClusteringColumns);
      for (String userColumn: userColumns) {
        if (!allColumns.contains(userColumn)) {
          allColumns.add(userColumn);
        }
      }
      columnCsv = COMMA_JOINER.join(allColumns);
    }

    // Add the token for the partition key to the SELECT statement.
    return COMMA_JOINER.join(mTokenColumn, columnCsv);
  }

  // package protected for testing...
  List<String> getPartitioningKeysForQuery(CqlQuerySpec query) {
    String keyspace = query.getKeyspace();
    String tableName = query.getTable();

    // Get the metadata for this table.
    TableMetadata tableMetadata = mSession
        .getCluster()
        .getMetadata()
        .getKeyspace(keyspace)
        .getTable(tableName);

    List<String> partitionKeyNames = Lists.newArrayList();
    for (ColumnMetadata columnMetadata : tableMetadata.getPartitionKey()) {
      partitionKeyNames.add(columnMetadata.getName());
    }
    return partitionKeyNames;
  }

  private List<Pair<String, DataType>> getColumnsToCheckAndTypes(Configuration conf) {
    // TODO: For now this is just token(partition key), but it could later include other columns.

    List<String> columnsForOrdering = ConfigHelper.getInputCqlQueryClusteringColumns(mConf);

    List<CqlQuerySpec> querySpecs = ConfigHelper.getInputCqlQueries(conf);
    CqlQuerySpec query = querySpecs.get(0);

    String keyspace = query.getKeyspace();
    String tableName = query.getTable();

    // Get the metadata for this table.
    TableMetadata tableMetadata = mSession
        .getCluster()
        .getMetadata()
        .getKeyspace(keyspace)
        .getTable(tableName);

    // Get a set of all of the clustering column names.
    Set<String> clusteringColumnNames = Sets.newHashSet();
    for (ColumnMetadata columnMetadata : tableMetadata.getClusteringColumns()) {
      clusteringColumnNames.add(columnMetadata.getName());
    }
    Preconditions.checkArgument(clusteringColumnNames.containsAll(columnsForOrdering));

    List<Pair<String, DataType>> columnsToCheck = Lists.newArrayList(
        Pair.of(mTokenColumn, DataType.bigint()));

    for (String clusteringColumn : columnsForOrdering) {
      ColumnMetadata columnMetadata = tableMetadata.getColumn(clusteringColumn);
      String name = columnMetadata.getName();
      DataType dataType = columnMetadata.getType();
      columnsToCheck.add(Pair.of(name, dataType));
    }
    return columnsToCheck;
  }

  /**
   * Execute all of our queries over the next token range in our list of token ranges for this
   * input split.
   *
   * If were are out of token ranges, then set the current row and the current row iterator both
   * to null.
   */
  private void queryNextTokenRange() {
    Preconditions.checkArgument(mTokenRangeIterator.hasNext());
    Preconditions.checkArgument(null == mMultiRowIterator || !mMultiRowIterator.hasNext());

    TokenRange nextTokenRange = mTokenRangeIterator.next();

    // Actually execute the query to create the row iterator!
    List<ResultSet> resultSets = executeQueries(
        nextTokenRange.getStartToken(),
        nextTokenRange.getEndToken()
    );

    mMultiRowIterator = new MultiRowIterator(
        resultSets,
        getColumnsToCheckAndTypes(mConf)
    );
  }

  public Session openSession(Configuration conf) {
    // TODO: Error checking if there are problems getting a session?
    // Create a session with a custom load-balancing policy that will ensure that we send queries
    // for system.local and system.peers to the same node.
    Cluster cluster = Cluster
        .builder()
        .addContactPoints(ConfigHelper.getInputNativeTransportContactPoints(conf))
        .withPort(ConfigHelper.getInputNativeTransportPort(conf))
        .withLoadBalancingPolicy(new ConsistentHostOrderPolicy())
        .build();
    return cluster.connect();
  }

  private List<ResultSet> executeQueries(String startToken, String endToken) {
    List<ResultSetFuture> futures = Lists.newArrayList();
    for (CqlQueryWithArgsToBind query : mCqlQueries) {
      futures.add(mSession.executeAsync(
          query.getStatementBoundExceptForTokenValues(
              Long.parseLong(startToken), Long.parseLong(endToken))
      ));
    }

    // Wait until all of the futures are done.
    List<ResultSet> results = new ArrayList<ResultSet>();

    for (ResultSetFuture resultSetFuture: futures) {
      results.add(resultSetFuture.getUninterruptibly());
    }
    return results;
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
    mSession.close();
  }

  // TODO: Make this return the partition key instead?  Or all of the columns to compare?
  // (Could use a sorted map...)
  /** {@inheritDoc} */
  public Text getCurrentKey() { return new Text("foo"); }

  /** {@inheritDoc} */
  public List<Row> getCurrentValue() {
    LOG.debug("Returning row value.");
    if (null == mCurrentRows) {
      LOG.debug("\tRow is null!");
    } else {
      LOG.debug("\tRow is not null!");
    }
    return mCurrentRows;
  }

  /** {@inheritDoc} */
  public float getProgress() {
    // TODO: Rewrite this...
    if (null == mCurrentRows) {
      return 0.0f;
    } else {
      return ((float) mRowCount) / mInputSplit.getLength();
    }
  }

  /** {@inheritDoc} */
  public boolean nextKeyValue() throws IOException {
    while(true) {
      if (mMultiRowIterator.hasNext()) {
        mCurrentRows = mMultiRowIterator.next();
        mRowCount += 1;
        return true;
      }
      // We are out of rows in the current token range.
      Preconditions.checkArgument(!mMultiRowIterator.hasNext());

      // We are also out of token ranges!
      if (!mTokenRangeIterator.hasNext()) {
        break;
      }

      // We still have more token ranges left!
      Preconditions.checkArgument(mTokenRangeIterator.hasNext());
      queryNextTokenRange();
    }

    mCurrentRows = null;
    return false;
  }

  // -----------------------------------------------------------------------------------------------
  // Methods for checking that the specified queries are valid.
  static void checkKeyspacesAndTablesExist(
      Session session, Collection<CqlQuerySpec> queries) throws IOException {
    Metadata metadata = session.getCluster().getMetadata();
    for (CqlQuerySpec query : queries) {
      String keyspace = query.getKeyspace();
      if (null == metadata.getKeyspace(keyspace)) {
        throw new IOException("Cannot find keyspace " + keyspace);
      }
      String table = query.getTable();
      if (null == metadata.getKeyspace(keyspace).getTable(table)) {
        throw new IOException("Cannot find table " + table);
      }
    }
  }

  static void checkParitionKeysAreIdentical(
      Session session, Collection<CqlQuerySpec> queries) throws IOException {
    List<ColumnMetadata> firstPartitionKey = null;
    for (CqlQuerySpec query : queries) {
      if (null == firstPartitionKey) {
        firstPartitionKey = getPartitionKeysForQuery(session, query);
        continue;
      }
      List<ColumnMetadata> partitionKey = getPartitionKeysForQuery(session, query);
      compareColumnMetadataLists(firstPartitionKey, partitionKey);
    }
  }

  static void compareColumnMetadataLists(
      List<ColumnMetadata> listA,
      List<ColumnMetadata> listB
  ) throws IOException {
    if (listA.size() != listB.size()) {
      // TODO: Better error message.
      throw new IOException("Number of columns to compare are not equal for all tables.");
    }

    for (int i = 0; i < listA.size(); i++) {
      ColumnMetadata columnA = listA.get(i);
      ColumnMetadata columnB = listB.get(i);

      if (!(columnA.getName().equals(columnB.getName()))) {
        throw new IOException("Columns with different names.");
      }

      if (columnA.getType() != columnB.getType()) {
        throw new IOException("Columns with different types.");
      }
    }
  }

  private static List<ColumnMetadata> getPartitionKeysForQuery(
      Session session, CqlQuerySpec query) {
    return session
        .getCluster()
        .getMetadata()
        .getKeyspace(query.getKeyspace())
        .getTable(query.getTable())
        .getPartitionKey();
  }

  private static class CqlQueryWithArgsToBind {
    private final PreparedStatement mStatement;

    private final List<Serializable> mArgsToBind;

    public CqlQueryWithArgsToBind(PreparedStatement statement, List<Serializable> argsToBind) {
      mStatement = statement;
      mArgsToBind = argsToBind;
    }

    public BoundStatement getStatementBoundExceptForTokenValues(long startToken, long endToken)  {
      // Need to do some inspection on the variables in the prepared statement to figure out how
      // to case the list of objects before binding.
      ColumnDefinitions columnDefinitions = mStatement.getVariables();
      Preconditions.checkArgument(columnDefinitions.size() == 2 + mArgsToBind.size());

      BoundStatement boundStatement = new BoundStatement(mStatement);
      int columnNumber = 0;

      // Loop through all of the non-token column, cast the arguments appropriately, and bind them
      // to the statement.
      for (ColumnDefinitions.Definition columnDef : columnDefinitions) {
        if (columnNumber == mArgsToBind.size()) {
          break;
        }
        // Figure out the class to which we need to cast our argument.
        DataType dataType = columnDef.getType();
        Class clazz = dataType.asJavaClass();

        // Actually perform the cast and update the bound statement.
        Object objectToBind = mArgsToBind.get(columnNumber);
        if (!(clazz.isInstance(objectToBind))) {
          throw new IllegalArgumentException(
              "Cannot case " + objectToBind + " to Java class " + clazz + " for CQL data type " +
                  dataType);
        }
        boundStatement = boundStatement.bind(clazz.cast(objectToBind));
        columnNumber++;
      }
      return boundStatement.bind(startToken, endToken);
    }
  }
}
