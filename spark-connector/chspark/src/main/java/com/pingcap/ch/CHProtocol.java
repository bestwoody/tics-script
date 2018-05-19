package com.pingcap.ch;

public class CHProtocol {
    public static class Server {
        static final int Hello = 0;                /// Name, version, revision.
        static final int Data = 1;                 /// A block of data (compressed or not).
        static final int Exception = 2;            /// The exception during query execution.
        static final int Progress = 3;             /// Query execution progress: rows read, bytes read.
        static final int Pong = 4;                 /// Ping response
        static final int EndOfStream = 5;          /// All packets were transmitted
        static final int ProfileInfo = 6;          /// Packet with profiling info.
        static final int Totals = 7;               /// A block with totals (compressed or not).
        static final int Extremes = 8;             /// A block with minimums and maximums (compressed or not).
        static final int TablesStatusResponse = 9; /// A response to TablesStatus request.
    }

    public static class Client {
        static final int Hello = 0;               /// Name, version, revision, default DB
        static final int Query = 1;               /// Query id, query settings, stage up to which the query must be executed,
                                                  /// whether the compression must be used,
                                                  /// query text (without data for INSERTs).
        static final int Data = 2;                /// A block of data (compressed or not).
        static final int Cancel = 3;              /// Cancel the query execution.
        static final int Ping = 4;                /// Check that connection to the server is alive.
        static final int TablesStatusRequest = 5; /// Check status of tables on the server.
    }

    /// Whether the compression must be used.
    public static class Compression {
        static final int Disable = 0;
        static final int Enable = 1;
    }

    /// Whether the ssl must be used.
    public static class Encryption {
        static final int Disable = 0;
        static final int Enable = 1;
    }
}
