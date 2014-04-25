package org.apache.cassandra.hadoop2.NativeInputFormat;

/**
* Created by clint on 4/2/14.
*/
class TokenRange {
  private final String startToken;
  private final String endToken;
  TokenRange(String startToken, String endToken) {
    this.startToken = startToken;
    this.endToken = endToken;
  }

  String getStartToken() {
    return startToken;
  }

  String getEndToken() {
    return endToken;
  }

  public String toString() {
    return String.format(
        "(%s, %s)",
        startToken,
        endToken
    );
  }
}
