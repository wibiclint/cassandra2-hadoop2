package org.apache.cassandra.hadoop2.multiquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run a few simple sanity checks for the input format.
 */
public class TestInputFormat extends BaseInputFormatTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestInputFormat.class);

  private static final String KEYSPACE_BIG = "big";

  private static final String TABLE_BIG = "big";

  private static final String COL_KEY = "foo";

  private static final String COL_VAL = "bar";

  private static final long NUM_VALUES = 10L;

  private Set<Integer> mDataInTable;

  private void createNewTable() {
    mSession.execute(String.format(
        "CREATE KEYSPACE IF NOT EXISTS %s " +
            "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };",
        KEYSPACE_BIG
    ));

    mSession.execute(String.format(
        "CREATE TABLE %s.%s (%s INT PRIMARY KEY, %s INT)",
        KEYSPACE_BIG,
        TABLE_BIG,
        COL_KEY,
        COL_VAL));
  }

  private void createLotsOfData() {
    Random random = new Random();
    mDataInTable = Sets.newHashSet();
    for (int i = 0; i < NUM_VALUES; i += 1) {
      int nextValue;

      do {
        nextValue = random.nextInt();
      } while (mDataInTable.contains(nextValue));

      mDataInTable.add(nextValue);
    }
    Preconditions.checkArgument(mDataInTable.size() == NUM_VALUES);
  }

  private void insertDataIntoTable() {
    PreparedStatement insertStatement = mSession.prepare(String.format(
        "INSERT INTO %s.%s (%s, %s) VALUES (?, ?)",
        KEYSPACE_BIG,
        TABLE_BIG,
        COL_KEY,
        COL_VAL
    ));
    for (int val : mDataInTable) {
      mSession.execute(insertStatement.bind(val, val));
    }
  }

  @Test
  public void testInputFormat() {
    // Create a new table and populate it with lots and lots of data.
    createNewTable();
    createLotsOfData();
    insertDataIntoTable();

    ConfigHelper.setInputCqlQuery(
        mConf,
        CqlQuerySpec.builder()
            .withKeyspace(KEYSPACE_BIG)
            .withTable(TABLE_BIG)
            .build()
    );

    MultiQueryCqlInputFormat inputFormat = new MultiQueryCqlInputFormat();

    List<InputSplit> inputSplits;
    try {
      inputSplits = inputFormat.getSplitsFromConf(mConf);
      MultiQueryRecordReader recordReader = new MultiQueryRecordReader();

      Set<Integer> readValues = Sets.newHashSet();

      for (InputSplit inputSplit : inputSplits) {
        recordReader.initializeWithConf(inputSplit, mConf);

        while (recordReader.nextKeyValue()) {
          List<Row> rows = recordReader.getCurrentValue();
          assertEquals(1, rows.size());
          int val = rows.get(0).getInt(COL_VAL);
          assertFalse(readValues.contains(val));
          readValues.add(val);
        }
      }
      assertEquals(mDataInTable, readValues);
    } catch (IOException ioe) {
      throw new AssertionError();
    }
  }
}
