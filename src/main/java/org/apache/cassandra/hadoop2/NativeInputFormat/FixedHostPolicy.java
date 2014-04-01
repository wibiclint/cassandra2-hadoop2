package org.apache.cassandra.hadoop2.NativeInputFormat;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Load balancing policy that sends requests always to a single host.
 */
public class FixedHostPolicy implements LoadBalancingPolicy {

  private static final Logger logger = LoggerFactory.getLogger(FixedHostPolicy.class);

  private final CopyOnWriteArrayList<Host> liveHosts = new CopyOnWriteArrayList<Host>();
  private final AtomicInteger index = new AtomicInteger();

  private QueryOptions queryOptions;
  private volatile boolean hasLoggedLocalCLUse;

  /** IP address for the fixed host to go to with all queries for this policy. */
  final private InetAddress fixedHostAddress;
  private Host fixedHost;

  public static FixedHostPolicy fromFixedHostAddress(InetAddress fixedHostAddress) {
    return new FixedHostPolicy(fixedHostAddress);
  }

  private FixedHostPolicy(InetAddress fixedHostAddress) {
    this.fixedHostAddress = fixedHostAddress;
    this.fixedHost = null;
  }

  @Override
  public void init(Cluster cluster, Collection<Host> hosts) {
    liveHosts.addAll(hosts);
    fixedHost = getHostThatMatchesInetAddress(hosts, fixedHostAddress);
    /*
    if (null == fixedHost) {
      throw new IOException(String.format(
          "Could not find host that matches specified fixed-host address %s",
          fixedHostAddress
      ));
    }
    */
    assert(null != fixedHost);
    queryOptions = cluster.getConfiguration().getQueryOptions();
  }

  private Host getHostThatMatchesInetAddress(Collection<Host> hosts, InetAddress address) {
    for (Host host: hosts) {
      if (host.getAddress().equals(address)) {
        return host;
      }
    }
    return null;
  }

  /**
   * Return the HostDistance for the provided host.
   * <p>
   * This policy consider all nodes as local. This is generally the right
   * thing to do in a single datacenter deployment.
   *
   * @param host the host of which to return the distance of.
   * @return the HostDistance to {@code host}.
   */
  @Override
  public HostDistance distance(Host host) {
    return HostDistance.LOCAL;
  }

  /**
   * Returns the hosts to use for a new query.
   * <p>
   *
   * @param loggedKeyspace the keyspace currently logged in on for this
   * query.
   * @param statement the query for which to build the plan.
   * @return a new query plan, i.e. an iterator indicating which host to
   * try first for querying, which one to use as failover, etc...
   */
  @Override
  public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
    return new AbstractIterator<Host>() {
      private int remaining = 1;

      @Override
      protected Host computeNext() {
        if (remaining <= 0)
          return endOfData();
        remaining--;

        return fixedHost;
      }
    };
  }

  @Override
  public void onUp(Host host) {
    liveHosts.addIfAbsent(host);
  }

  @Override
  public void onDown(Host host) {
    //assert(host != fixedHost);
    liveHosts.remove(host);
  }

  @Override
  public void onAdd(Host host) {
    onUp(host);
  }

  @Override
  public void onRemove(Host host) {
    onDown(host);
  }
}
