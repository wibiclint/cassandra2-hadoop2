package org.apache.cassandra.hadoop2.multiquery;

/**
* Describes a token range associated with a subsplit.
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
