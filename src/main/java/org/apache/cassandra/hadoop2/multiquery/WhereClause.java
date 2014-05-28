package org.apache.cassandra.hadoop2.multiquery;

import java.io.Serializable;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
* Created by clint on 5/28/14.
*/
public class WhereClause implements Serializable {
  private final String mClause;
  private final List<Serializable> mArgs;

  public WhereClause(String clause, List<Serializable> args) {
    mClause = clause;
    Preconditions.checkNotNull(args);
    mArgs = args;
    Preconditions.checkNotNull(mArgs, "Null list from args " + args);

    // TODO: Check that the number of ?s in the statement is the same as the number of args.
  }

  public String getClause() {
    return mClause;
  }

  public List<Serializable> getArgs() {
    return mArgs;
  }

}
