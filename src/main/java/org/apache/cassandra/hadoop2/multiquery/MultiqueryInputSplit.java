/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.hadoop2.multiquery;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

class MultiQueryInputSplit extends InputSplit implements Writable {
  private List<TokenRange> tokenRanges;
  private List<String> hosts;

  // TODO: Is there a better answer here?
  private static final long SPLIT_LENGTH = 1L;

  /** Not totally sure why we need this... */
  public MultiQueryInputSplit() {}

  public static MultiQueryInputSplit createFromSubplit(Subsplit subsplit) {
    return new MultiQueryInputSplit(
        Lists.newArrayList(new TokenRange(subsplit.startToken, subsplit.endToken)),
        Lists.newArrayList(subsplit.getHosts())
    );
  }

  public static MultiQueryInputSplit createFromSubplits(Collection<Subsplit> subsplits) {
    List<TokenRange> tokenRanges = Lists.newArrayList();
    Set<String> hosts = Sets.newHashSet();
    for (Subsplit subsplit : subsplits) {
      tokenRanges.add(new TokenRange(subsplit.getStartToken(), subsplit.getEndToken()));
      hosts.addAll(subsplit.getHosts());
    }
    return new MultiQueryInputSplit(tokenRanges, Lists.newArrayList(hosts));
  }

  private MultiQueryInputSplit(List<TokenRange> tokenRanges, List<String> hosts) {
    this.tokenRanges = tokenRanges;
    this.hosts = hosts;
  }

  public String[] getLocations() {
    return hosts.toArray(new String[hosts.size()]);
  }

  public long getLength() {
    return SPLIT_LENGTH;
  }

  // These three methods are for serializing and deserializing
  // KeyspaceSplits as needed by the Writable interface.
  public void write(DataOutput out) throws IOException {
    out.writeInt(tokenRanges.size());
    for (TokenRange tokenRange : tokenRanges) {
      out.writeUTF(tokenRange.getStartToken());
      out.writeUTF(tokenRange.getEndToken());
    }
    out.writeInt(hosts.size());
    for (String endpoint : hosts) {
      out.writeUTF(endpoint);
    }
  }

  public void readFields(DataInput in) throws IOException {
    int numTokenRanges = in.readInt();
    tokenRanges = Lists.newArrayList();
    for (int i = 0; i < numTokenRanges; i++) {
      String startToken = in.readUTF();
      String endToken = in.readUTF();
      tokenRanges.add(new TokenRange(startToken, endToken));
    }
    int numOfEndpoints = in.readInt();
    hosts = Lists.newArrayList();
    for (int i = 0; i < numOfEndpoints; i++) {
      hosts.add(in.readUTF());
    }
  }

  @Override
  public String toString() {
    return String.format(
        "MultiqueryInputSplit(%s)",
        hosts
    );
  }

  public Iterator<TokenRange> getTokenRangeIterator() {
    return tokenRanges.iterator();
  }
}
