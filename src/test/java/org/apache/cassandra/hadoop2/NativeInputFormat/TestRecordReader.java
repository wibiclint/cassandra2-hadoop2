package org.apache.cassandra.hadoop2.NativeInputFormat;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Test;

public class TestRecordReader extends BaseInputFormatTest {
  @Test
  public void testBasicRecordReader() {
    // Very basic query, just select everything from the logos table.
    NewCqlConfigHelper.setInputCqlQuery(mConf, KEYSPACE, TABLE_LOGOS, "logo");

    // Create a single token range for the entire ring.
    Subsplit subsplit = Subsplit.createFromHost(
        Long.toString(Subsplit.RING_START_TOKEN),
        Long.toString(Subsplit.RING_END_TOKEN),
        "127.0.0.1");

    CqlInputSplit split = CqlInputSplit.createFromSubplit(subsplit);
    CqlRecordReader recordReader = new CqlRecordReader();

    try {
      recordReader.initializeWithConf(split, mConf);

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
      // ...
    }

  }
}
