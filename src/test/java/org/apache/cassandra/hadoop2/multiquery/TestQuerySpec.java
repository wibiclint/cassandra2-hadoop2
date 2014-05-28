package org.apache.cassandra.hadoop2.multiquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestQuerySpec {
  private static final Logger LOG = LoggerFactory.getLogger(TestQuerySpec.class);

  @Test
  public void testSerDes() {
    final String STRING_TO_BIND = "value to bind";
    final long LONG_TO_BIND = 10L;
    CqlQuerySpec query = CqlQuerySpec.builder()
        .withColumns("A", "B")
        .withKeyspace("keyspace")
        .withTable("table")
        .withWhereClause("WHERE foo = ? and bar = ?", STRING_TO_BIND, LONG_TO_BIND)
        .build();

    assertNotNull(query.getWhereClauses().getArgs());

    Configuration conf = new Configuration();
    ConfigHelper.setInputCqlQuery(conf, query);

    CqlQuerySpec readQuery = ConfigHelper.getInputCqlQueries(conf).get(0);
    assertNotNull(readQuery);

    WhereClause whereClause = readQuery.getWhereClauses();
    List<Serializable> args = whereClause.getArgs();
    assertEquals(2, args.size());
    assertTrue(args.get(0) instanceof String);
    assertEquals(STRING_TO_BIND, (String) args.get(0));
    assertTrue(args.get(1) instanceof Long);
    assertEquals(LONG_TO_BIND, ((Long) args.get(1)).longValue());
  }

}
