CREATE TABLE IF NOT EXISTS partsupp( 
	PS_PARTKEY     UInt8,
        PS_SUPPKEY     UInt8,
        PS_AVAILQTY    UInt8,
        PS_SUPPLYCOST  Float64,
        PS_COMMENT     String 
)ENGINE = Log;

