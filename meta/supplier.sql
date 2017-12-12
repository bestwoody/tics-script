CREATE TABLE IF NOT EXISTS supplier (
	S_SUPPKEY     UInt8,
	S_NAME        String,
	S_ADDRESS     String,
	S_NATIONKEY   UInt8,
	S_PHONE       String,
	S_ACCTBAL     Float64,
	S_COMMENT     String
	)ENGINE = Log;
