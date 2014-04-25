package org.apache.cassandra.hadoop2.NativeInputFormat;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.base.Joiner;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator that operates over multiple Cassandra Row Iterators at once.
 *
 * An instance of this class will group together rows that are identical over a given set of
 * columns (e.g., the partitioning columns, the clustering columns, etc.).  The list of columns to
 * use for comparing must contain *at least* all of the partitioning columns.
 *
 * The input rows are assumed to be ordered by these collections of columns in the iterators.
 *
 * TODO: Use a merge sort over the iterators to group together rows that have the same values for
 * the columns that we care about.  Sort first by token, and then by column values.
 *
 * The primary key must be the same for all of the rows.
 */
public class MultiRowIterator implements Iterator<List<Row>> {
  private static final Logger LOG = LoggerFactory.getLogger(MultiRowIterator.class);

  private final PeekingIterator<Row> mRowIterator;
  private final List<Pair<String, DataType>> mColumnsToCompare;
  private final RowComparator mRowComparator;

  /**
   * Create a multi-row iterator.
   *
   * @param resultSets A list of result sets.  We will return rows from this iterator in the same
   * order as the result sets.
   * @param columnsToCompare A list of columns, in order, for comparing and sorting rows.
   */
  public MultiRowIterator(
      List<ResultSet> resultSets,
      // TODO: Maybe use ColumnMetadata here instead of String?
      // Would allow storing "type" as well as name...
      List<Pair<String, DataType>> columnsToCompare) {
    List<PeekingIterator<Row>> rowIterators = Lists.newArrayList();
    for (ResultSet resultSet : resultSets) {
      rowIterators.add(Iterators.peekingIterator(resultSet.iterator()));
    }

    mColumnsToCompare = columnsToCompare;

    mRowComparator = new RowComparator();
    mRowIterator = Iterators.peekingIterator(Iterators.mergeSorted(rowIterators, mRowComparator));

  }

  /** {@inheritDoc} */
  public boolean hasNext() {
    return mRowIterator.hasNext();
  }

  /** {@inheritDoc} */
  public List<Row> next() {
    // Get the first row in our iterator.
    Row firstRow = mRowIterator.next();

    LOG.debug("First row = " + firstRow);

    List<Row> rowsToReturnTogether = Lists.newArrayList(firstRow);

    // Continue popping rows off of the iterator as long as all of the columns that we want to
    // compare are the same between this row and the next.
    while (mRowIterator.hasNext() &&
        mRowComparator.compare(firstRow, mRowIterator.peek()) == 0) {
      LOG.debug("Next row = " + mRowIterator.peek());
      rowsToReturnTogether.add(mRowIterator.next());
    }

    // Return all of these rows that had the same partition key + set of clustering columns.
    return rowsToReturnTogether;
  }

  /** {@inheritDoc} */
  public void remove() {
    throw new UnsupportedOperationException("Cannot remove from this iterator!");
  }
  // -----------------------------------------------------------------------------------------------

  /**
   * Compares {@link com.datastax.driver.core.Row} objects by their Kiji Entity ID. The Row objects
   * must be from the same table.  The Rows are first compared by their partion key token, and then
   * by the entity ID components they contain.
   */
  private final class RowComparator implements Comparator<Row> {

    /** {@inheritDoc} */
    @Override
    public int compare(Row o1, Row o2) {
      ComparisonChain chain =  ComparisonChain.start();
      for (Pair<String, DataType> columnAndType : mColumnsToCompare) {
        String columnName = columnAndType.getLeft();
        DataType dataType = columnAndType.getRight();

        switch (dataType.getName()) {
          case BOOLEAN:
            chain = chain.compare(
                Boolean.toString(o1.getBool(columnName)),
                Boolean.toString(o2.getBool(columnName)));
            break;
          case INT:
            chain = chain.compare(o1.getInt(columnName), o2.getInt(columnName));
            break;
          case BIGINT:
          case COUNTER:
            chain = chain.compare(o1.getLong(columnName), o2.getLong(columnName));
            break;
          case DOUBLE:
            chain = chain.compare(o1.getDouble(columnName), o2.getDouble(columnName));
            break;
          case FLOAT:
            chain = chain.compare(o1.getFloat(columnName), o2.getFloat(columnName));
            break;
          case BLOB:
            chain = chain.compare(o1.getBytes(columnName), o2.getBytes(columnName));
            break;
          case VARCHAR:
          case TEXT:
          case ASCII:
            chain = chain.compare(o1.getString(columnName), o2.getString(columnName));
            break;
          case VARINT:
            chain = chain.compare(o1.getVarint(columnName), o2.getVarint(columnName));
            break;
          case DECIMAL:
            chain = chain.compare(o1.getDecimal(columnName), o2.getDecimal(columnName));
            break;
          case UUID:
          case TIMEUUID:
            chain = chain.compare(o1.getUUID(columnName), o2.getUUID(columnName));
            break;
          case INET:
            chain = chain.compare(
                o1.getInet(columnName).toString(), o2.getInet(columnName).toString());
            break;
          default:
            throw new UnsupportedOperationException("Cannot sort by " + dataType.getName() + "!");
        }
      }
      return chain.result();
    }
  }
}
