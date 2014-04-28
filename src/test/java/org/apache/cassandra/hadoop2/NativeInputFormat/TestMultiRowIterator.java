package org.apache.cassandra.hadoop2.NativeInputFormat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import junit.framework.Assert;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the multi-row iterator.
 */
public class TestMultiRowIterator extends BaseInputFormatTest {

  @Test
  public void testGroupByOneColumn() {
    NewCqlConfigHelper.setInputCqlQuery(
        mConf,
        CqlQuerySpec.builder().withKeyspace(KEYSPACE).withTable(TABLE_LOGOS).build());

    ResultSet resultSet = mSession.execute(String.format(
        "SELECT * from %s.%s",
        KEYSPACE,
        TABLE_LOGOS
    ));
    List<ResultSet> resultSets = Lists.newArrayList();
    resultSets.add(resultSet);

    List<Pair<String, DataType>> columns = Lists.newArrayList();
    columns.add(Pair.of(COL_STATE, DataType.text()));

    MultiRowIterator multiRowIterator = new MultiRowIterator(resultSets, columns);

    assertTrue(multiRowIterator.hasNext());

    // We are grouping by state, so we should get one set of rows for CA, DC, IL, TX.
    List<List<Row>> rowsByState = Lists.newArrayList(multiRowIterator);

    assertEquals(4, rowsByState.size());

    for (List<Row> rowsForOneState : rowsByState) {
      String state = rowsForOneState.get(0).getString(COL_STATE);
      if (state.equals("CA") || state.equals("TX")) {
        assertEquals(3, rowsForOneState.size());
      } else if (state.equals("DC") || state.equals("IL")) {
        assertEquals(1, rowsForOneState.size());
      } else {
        assertFalse(true);
      }
    }
  }

  @Test
  public void testGroupByTwoColumns() {
    NewCqlConfigHelper.setInputCqlQuery(mConf,
        CqlQuerySpec.builder().withKeyspace(KEYSPACE).withTable(TABLE_LOGOS).build());

    ResultSet resultSet = mSession.execute(String.format(
        "SELECT * from %s.%s",
        KEYSPACE,
        TABLE_LOGOS
    ));
    List<ResultSet> resultSets = Lists.newArrayList();
    resultSets.add(resultSet);

    List<Pair<String, DataType>> columns = Lists.newArrayList();
    columns.add(Pair.of(COL_STATE, DataType.text()));
    columns.add(Pair.of(COL_CITY, DataType.text()));

    MultiRowIterator multiRowIterator = new MultiRowIterator(resultSets, columns);

    assertTrue(multiRowIterator.hasNext());

    // We are grouping by state and city, so we should get seven lists (LA has two teams!).
    List<List<Row>> rowsByState = Lists.newArrayList(multiRowIterator);

    assertEquals(7, rowsByState.size());

    for (List<Row> rowsForOneState : rowsByState) {
      if (rowsForOneState.get(0).getString(COL_CITY).equals("Los Angeles")) {
        assertEquals(2, rowsForOneState.size());
      } else {
        assertEquals(1, rowsForOneState.size());
      }
    }
  }
}
