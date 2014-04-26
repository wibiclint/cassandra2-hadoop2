package org.apache.cassandra.hadoop2.NativeInputFormat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRecordReader extends BaseInputFormatTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestRecordReader.class);

  private CqlInputSplit getCqlInputSplit() {
    // Create a single token range for the entire ring.
    Subsplit subsplit = Subsplit.createFromHost(
        Long.toString(Subsplit.RING_START_TOKEN),
        Long.toString(Subsplit.RING_END_TOKEN),
        "127.0.0.1");

    CqlInputSplit split = CqlInputSplit.createFromSubplit(subsplit);
    return split;
  }

  @Test
  public void testBasicRecordReader() {
    // Very basic query, just select everything from the logos table.
    NewCqlConfigHelper.setInputCqlQuery(mConf, KEYSPACE, TABLE_LOGOS, COL_LOGO);
    CqlRecordReader recordReader = new CqlRecordReader();

    try {
      recordReader.initializeWithConf(getCqlInputSplit(), mConf);

      // Partition key here is just the state.
      CqlQuerySpec querySpec = NewCqlConfigHelper.getInputCqlQueries(mConf).get(0);
      List<String> partitioningKeys = recordReader.getPartitioningKeysForQuery(querySpec);
      assertEquals(1, partitioningKeys.size());

      // The RecordReader should group by primary key, so we should see a different List<Row> for
      // each unique state.
      int stateCount = 0;
      while(true) {
        if (!recordReader.nextKeyValue()) {
          break;
        }
        stateCount += 1;
      }
      assertEquals(NUM_STATES, stateCount);

    } catch (IOException ioe) {
      assertFalse("Should not get here.", false);
    }
  }

  @Test
  public void testSelectAll() {
    // Very basic query, just select everything from the logos table.
    NewCqlConfigHelper.setInputCqlQuery(mConf, KEYSPACE, TABLE_LOGOS, "*");
    CqlRecordReader recordReader = new CqlRecordReader();

    try {
      recordReader.initializeWithConf(getCqlInputSplit(), mConf);

      assertTrue(recordReader.nextKeyValue());
      List<Row> rowsForState = recordReader.getCurrentValue();
      try {
        rowsForState.get(0).getString(COL_CITY);
        assertTrue("Should get here.", true);
      } catch (IllegalArgumentException iae) {
        assertFalse("Should not get here.", false);
      }
    } catch (IOException ioe) {
      assertFalse("Should not get here.", false);
    }
  }

  @Test
  public void testGroupWithClusteringColumn() {
    // Very basic query, just select everything from the logos table.
    NewCqlConfigHelper.setInputCqlQuery(mConf, KEYSPACE, TABLE_LOGOS, COL_LOGO);
    NewCqlConfigHelper.setInputCqlQueryClusteringColumnsCsv(mConf, COL_CITY);
    CqlRecordReader recordReader = new CqlRecordReader();

    try {
      recordReader.initializeWithConf(getCqlInputSplit(), mConf);

      // Partition key here is just the state.
      CqlQuerySpec querySpec = NewCqlConfigHelper.getInputCqlQueries(mConf).get(0);
      List<String> partitioningKeys = recordReader.getPartitioningKeysForQuery(querySpec);
      assertEquals(1, partitioningKeys.size());

      // The RecordReader should group by primary key, so we should see a different List<Row> for
      // each unique state.
      int stateCount = 0;
      while(true) {
        if (!recordReader.nextKeyValue()) {
          break;
        }
        stateCount += 1;
      }
      assertEquals(NUM_CITIES, stateCount);

    } catch (IOException ioe) {
      assertFalse("Should not get here.", false);
    }
  }

  @Test
  public void testTwoQueries() {
    // Select everything from logos and everything from players.
    NewCqlConfigHelper.setInputCqlQuery(mConf, KEYSPACE, TABLE_LOGOS, COL_LOGO);
    NewCqlConfigHelper.setInputCqlQuery(mConf, KEYSPACE, TABLE_PLAYERS, COL_PLAYER);
    CqlRecordReader recordReader = new CqlRecordReader();

    try {
      recordReader.initializeWithConf(getCqlInputSplit(), mConf);

      // Partition key here is just the state.
      CqlQuerySpec querySpec = NewCqlConfigHelper.getInputCqlQueries(mConf).get(0);
      List<String> partitioningKeys = recordReader.getPartitioningKeysForQuery(querySpec);
      assertEquals(1, partitioningKeys.size());

      // The RecordReader should group by primary key, so we should see a different List<Row> for
      // each unique state.
      int stateCount = 0;
      while(true) {
        if (!recordReader.nextKeyValue()) {
          break;
        }
        stateCount += 1;
      }
      assertEquals(NUM_STATES, stateCount);

    } catch (IOException ioe) {
      assertFalse("Should not get here.", false);
    }
  }

  @Test
  public void testTwoQueriesWithClusteringColumns() {
    // Select from logos and from players.
    NewCqlConfigHelper.setInputCqlQuery(mConf, KEYSPACE, TABLE_LOGOS, COL_LOGO);
    NewCqlConfigHelper.setInputCqlQuery(mConf, KEYSPACE, TABLE_PLAYERS, COL_PLAYER);

    // Cluster by team!
    NewCqlConfigHelper.setInputCqlQueryClusteringColumnsCsv(mConf, COL_CITY, COL_TEAM);
    CqlRecordReader recordReader = new CqlRecordReader();

    try {
      recordReader.initializeWithConf(getCqlInputSplit(), mConf);

      // Partition key here is just the state.
      CqlQuerySpec querySpec = NewCqlConfigHelper.getInputCqlQueries(mConf).get(0);
      List<String> partitioningKeys = recordReader.getPartitioningKeysForQuery(querySpec);
      assertEquals(1, partitioningKeys.size());

      // The RecordReader should group by team, so we should see a different List<Row> for each
      // team.
      int teamCount = 0;
      boolean haveSeenWizards = false;
      while(true) {
        if (!recordReader.nextKeyValue()) {
          break;
        }
        List<Row> rowsThisTeam = recordReader.getCurrentValue();

        // If this is the Wizards, then we should see Wall and Beal.
        Row row0 = rowsThisTeam.get(0);
        if (row0.getString(COL_TEAM).equals("Wizards")) {
          assertFalse(haveSeenWizards);
          haveSeenWizards = true;

          // We should have one row for John Wall, one for Bradley Beal, and one for the logo.
          assertEquals(3, rowsThisTeam.size());

          boolean haveSeenWall = false;
          boolean haveSeenBeal = false;
          boolean haveSeenLogo = false;

          // Check to make sure that we get the rows that we expect.
          for (Row row : rowsThisTeam) {
            ColumnDefinitions columns = row.getColumnDefinitions();
            if (columns.contains(COL_LOGO) && row.getString(COL_LOGO).equals("Wizard")) {
              assertFalse(haveSeenLogo);
              haveSeenLogo = true;
            }

            if (columns.contains(COL_PLAYER) && row.getString(COL_PLAYER).equals("Wall")) {
              assertFalse(haveSeenWall);
              haveSeenWall = true;
            }

            if (columns.contains(COL_PLAYER) && row.getString(COL_PLAYER).equals("Beal")) {
              assertFalse(haveSeenBeal);
              haveSeenBeal = true;
            }
          }
          assertTrue(haveSeenBeal);
          assertTrue(haveSeenLogo);
          assertTrue(haveSeenWall);
        }
        teamCount += 1;
      }
      assertTrue(haveSeenWizards);
      assertEquals(NUM_TEAMS, teamCount);

    } catch (IOException ioe) {
      assertFalse("Should not get here.", false);
    }
  }
}
