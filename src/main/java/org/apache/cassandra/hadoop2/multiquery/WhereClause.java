package org.apache.cassandra.hadoop2.multiquery;

import java.io.Serializable;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WhereClause implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(WhereClause.class);
  private final String mClause;
  private final List<Serializable> mArgs;

  public WhereClause(String clause, List<Serializable> args) {
    mClause = clause;
    Preconditions.checkNotNull(args);
    mArgs = args;
    Preconditions.checkNotNull(mArgs, "Null list from args " + args);

    // TODO: Check that the number of ?s in the statement is the same as the number of args.
    // TODO: Create real builder interface for adding arguments.
    // TODO: Check that all arguments are one of the primitive types allowable for CQL.

    LOG.debug("Created WHERE clause");
    LOG.debug("Clause = " + mClause);
    LOG.debug("Values to bind:");
    for (Serializable arg : mArgs) {
      LOG.debug("\t" + arg.toString());
    }
  }

  public String getClause() {
    return mClause;
  }

  public List<Serializable> getArgs() {
    return mArgs;
  }

}
