package org.apache.cassandra.hadoop2.NativeInputFormat;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;

import java.util.*;

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
    final int numSubsplits = subsplits.size();
    final int numSubsplitsPerSplit =
        numSubsplits / NewCqlConfigHelper.getDefaultInputTargetNumSplits(conf);

    // Group subsplits by host and try to combine subsplits that share a host.
    List<Subsplit> subsplitsSortedByHost = getSubsetsSortedByHost(subsplits);

    List<CqlInputSplit> inputSplits = Lists.newArrayList();

    int subsplitIndex = 0;
    while (subsplitIndex < numSubsplits) {

      // Start a new InputSplit.
      Set<Subsplit> subsplitsToCombine = Sets.newHashSet();

      // Go until we get to our target number of subsplits / input split.
      while (true) {
        // No more data => can't add to this InputSplit anymore.
        if (subsplitIndex >= numSubsplits) {
          break;
        }

        // Add this subsplit to the current working input split.
        Subsplit subsplitToAdd = subsplitsSortedByHost.get(subsplitIndex);
        subsplitsToCombine.add(subsplitToAdd);
        subsplitIndex++;

        // If we have reached our size goal, then finish this input split.
        if (subsplitsToCombine.size() == numSubsplitsPerSplit) {
          break;
        }
      }

      assert(subsplitsToCombine.size() > 0);

      // Now create the input split.
      CqlInputSplit inputSplit = CqlInputSplit.createFromSubplits(subsplitsToCombine);
      inputSplits.add(inputSplit);
    }
    return inputSplits;
  }

  private List<Subsplit> getSubsetsSortedByHost(Collection<Subsplit> unsortedSubsplits) {
    List<Subsplit> subsplitsSortedByHost = Lists.newArrayList(unsortedSubsplits);
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
    return subsplitsSortedByHost;
  }
}
