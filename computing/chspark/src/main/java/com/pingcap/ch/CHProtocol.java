package com.pingcap.ch;

public class CHProtocol {
  public static class Server {
    public static final int Hello = 0; // / Name, version, revision.
    public static final int Data = 1; // / A block of data (compressed or not).
    public static final int Exception = 2; // / The exception during query execution.
    public static final int Progress = 3; // / Query execution progress: rows read, bytes read.
    public static final int Pong = 4; // / Ping response
    public static final int EndOfStream = 5; // / All packets were transmitted
    public static final int ProfileInfo = 6; // / Packet with profiling info.
    public static final int Totals = 7; // / A block with totals (compressed or not).
    public static final int Extremes =
        8; /// A block with minimums and maximums (compressed or not).
    public static final int TablesStatusResponse = 9; // / A response to TablesStatus request.
    public static final int LockInfos = 100; // / Lock infos of some pending transactions.
    public static final int RegionException = 101; // / The exception during query execution.
  }

  public static class Client {
    public static final int Hello = 0; // / Name, version, revision, default DB
    public static final int Query =
        1; /// Query id, query settings, stage up to which the query must be executed,
    /// whether the compression must be used,
    /// query text (without data for INSERTs).
    public static final int Data = 2; // / A block of data (compressed or not).
    public static final int Cancel = 3; // / Cancel the query execution.
    public static final int Ping = 4; // / Check that connection to the server is alive.
    public static final int TablesStatusRequest = 5; // / Check status of tables on the server.
  }

  /// Whether the compression must be used.
  public static class Compression {
    public static final int Disable = 0;
    public static final int Enable = 1;
  }

  /// Whether the ssl must be used.
  public static class Encryption {
    public static final int Disable = 0;
    public static final int Enable = 1;
  }
}
