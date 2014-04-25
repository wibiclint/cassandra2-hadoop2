package org.apache.cassandra.hadoop2.NativeInputFormat;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.base.Joiner;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;

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
  private final PeekingIterator<Row> mRowIterator;
  private final List<String> mPartitionKey;
  private final List<String> mClusteringColumns;
  private final RowComparator mRowComparator;

  /**
   * Create a multi-row iterator.
   *
   * @param resultSets A list of result sets.  We will return rows from this iterator in the same
   * order as the result sets.
   * @param partitionKey A list of columns that for the partition key (must be same for all tables).
   * @param clusteringColumns A list of additional clustering columns to use for grouping rows
   * (must be same for all tables).
   */
  public MultiRowIterator(
      List<ResultSet> resultSets,
      // TODO: Maybe use ColumnMetadata here instead of String?
      // Would allow storing "type" as well as name...
      List<String> partitionKey,
      List<String> clusteringColumns) {
    List<PeekingIterator<Row>> rowIterators = Lists.newArrayList();
    for (ResultSet resultSet : resultSets) {
      rowIterators.add(Iterators.peekingIterator(resultSet.iterator()));
    }

    mRowComparator = new RowComparator(partitionKey, clusteringColumns);
    mRowIterator = Iterators.peekingIterator(Iterators.mergeSorted(rowIterators, mRowComparator));

    mPartitionKey = partitionKey;
    mClusteringColumns = clusteringColumns;
  }

  /** {@inheritDoc} */
  public boolean hasNext() {
    return mRowIterator.hasNext();
  }

  /** {@inheritDoc} */
  public List<Row> next() {
    // Get the first row in our iterator.
    Row firstRow = mRowIterator.next();

    List<Row> rowsToReturnTogether = Lists.newArrayList(firstRow);

    // Continue popping rows off of the iterator as long as all of the columns that we want to
    // compare are the same between this row and the next.
    while (mRowIterator.hasNext() &&
        mRowComparator.compare(firstRow, mRowIterator.peek()) == 0) {
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
  private static final class RowComparator implements Comparator<Row> {
    private final String mTokenColumn;
    private final List<String> mClusteringColumns;
    private static final Joiner COMMA_JOINER = Joiner.on(", ");

    private RowComparator(List<String> partitionKey, List<String> clusteringColumns) {
      mTokenColumn = String.format("token(%s)", COMMA_JOINER.join(partitionKey));
      mClusteringColumns = clusteringColumns;
    }

    /** {@inheritDoc} */
    @Override
    public int compare(Row o1, Row o2) {
      ComparisonChain chain =  ComparisonChain.start()
          .compare(o1.getLong(mTokenColumn), o2.getLong(mTokenColumn));
      for (String columnName : mClusteringColumns) {
        // TODO: Add more data types here...
        try {
          String s1 = o1.getString(columnName);
          String s2 = o2.getString(columnName);
          chain = chain.compare(s1, s2);
        } catch (InvalidTypeException ite) {
          // Do nothing...
        }
      }
      return chain.result();
    }
  }
}
