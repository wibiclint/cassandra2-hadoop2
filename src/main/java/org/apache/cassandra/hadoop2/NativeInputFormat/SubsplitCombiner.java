package org.apache.cassandra.hadoop2.NativeInputFormat;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;

/**
 * Combines subsplits into InputSplits.
 *
 * This class attempts to combine subplits such that:
 * <ul>
 *   <li>The final set of InputSplits matches the user's requested number of InputSplits</li>
 *   <li>As many subsplits as possible share a common replica node</li>
 * </ul>
 */
public class SubsplitCombiner {
  private final Configuration conf;

  /**
   * Constructor.
   * @param conf The Hadoop configuration with information about the Cassandra cluster.
   */
  public SubsplitCombiner(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Combine subsplits into InputSplits, attempting to group together subsplits that share replica
   * nodes.
   * @param subsplits A collection of subsplits to combine.
   * @return A list of InputSplits.
   */
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

  /**
   * Sort subsplits by host.
   *
   * @param unsortedSubsplits An unsorted collection of subsplits.
   * @return A list of the subsplits, sorted by host.
   */
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
