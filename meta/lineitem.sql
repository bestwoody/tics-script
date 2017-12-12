CREATE TABLE IF NOT EXISTS lineitem ( 
	L_ORDERKEY    UInt8,
	L_PARTKEY     UInt8,
        L_SUPPKEY     UInt8,
        L_LINENUMBER  UInt8,
        L_QUANTITY    Float32,
        L_EXTENDEDPRICE  Float32,
        L_DISCOUNT    Float64,
        L_TAX         Float64,
        L_RETURNFLAG  FixedString(1),
        L_LINESTATUS  FixedString(1),
        L_SHIPDATE    Date,
        L_COMMITDATE  Date,
        L_RECEIPTDATE Date,
        L_SHIPINSTRUCT String,
        L_SHIPMODE     String,
        L_COMMENT      String
	) ENGINE = MergeTree(L_SHIPDATE,(L_ORDERKEY,L_SHIPDATE),8192);

