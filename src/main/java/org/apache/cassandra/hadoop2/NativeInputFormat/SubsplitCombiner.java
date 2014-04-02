package org.apache.cassandra.hadoop2.NativeInputFormat;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Combines subsplits into InputSplits.
 */
public class SubsplitCombiner {
  private final Configuration conf;

  public SubsplitCombiner(Configuration conf) {
    this.conf = conf;
  }

  public List<CqlInputSplit> combineSubsplits(Collection<Subsplit> subsplits) {
    // Estimate the number of subsplits per input split.
    int numSubsplitsPerSplit =
        subsplits.size() / NewCqlConfigHelper.getDefaultInputTargetNumSplits(conf);

    // Group subsplits by host and try to combine subsplits that share a host.
    List<Subsplit> subsplitsSortedByHost = Lists.newArrayList(subsplits);

    Collections.sort(
        subsplitsSortedByHost,
        new Comparator<Subsplit>() {
          public int compare(Subsplit firstSubsplit, Subsplit secondSubsplit) {
            String firstHostList = firstSubsplit.getSortedHostListAsString();
            String secondHostList = secondSubsplit.getSortedHostListAsString();
            return firstHostList.compareTo(secondHostList);
          }
        }
    );

  }

}
